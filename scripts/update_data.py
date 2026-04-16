# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "geoparquet-io>=1.0.0b2",
#     "gpio-pmtiles",
#     "duckdb>=1.2.0",
#     "httpx",
#     "numpy",
#     "typer",
# ]
# ///
"""Download ANNCSU Italian address dataset (per region) and convert to GeoParquet.

Usage:
    uv run scripts/update_data.py
    uv run scripts/update_data.py --force   # skip freshness check

The script downloads all 20 regional ZIP files from the ANNCSU open data
portal, extracts the CSVs, merges them in DuckDB, creates point geometries
from coordinates, and exports the result as a spatially-sorted GeoParquet file.

Regional downloads replace the single national URL which was found to be
incomplete/outdated for some municipalities.
"""

import io
import re
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import typer

BASE_URL = "https://anncsu.open.agenziaentrate.gov.it/age-inspire/opendata/anncsu/getds.php?"
NATIONAL_URL = BASE_URL + "INDIR_ITA"

REGION_URLS = {
    "ABRU": BASE_URL + "INDIR_ABRU",
    "BASI": BASE_URL + "INDIR_BASI",
    "CALA": BASE_URL + "INDIR_CALA",
    "CAMP": BASE_URL + "INDIR_CAMP",
    "EMIL": BASE_URL + "INDIR_EMIL",
    "FRIU": BASE_URL + "INDIR_FRIU",
    "LAZI": BASE_URL + "INDIR_LAZI",
    "LIGU": BASE_URL + "INDIR_LIGU",
    "LOMB": BASE_URL + "INDIR_LOMB",
    "MARC": BASE_URL + "INDIR_MARC",
    "MOLI": BASE_URL + "INDIR_MOLI",
    "PIEM": BASE_URL + "INDIR_PIEM",
    "PUGL": BASE_URL + "INDIR_PUGL",
    "SARD": BASE_URL + "INDIR_SARD",
    "SICI": BASE_URL + "INDIR_SICI",
    "TOSC": BASE_URL + "INDIR_TOSC",
    "TREN": BASE_URL + "INDIR_TREN",
    "UMBR": BASE_URL + "INDIR_UMBR",
    "VALL": BASE_URL + "INDIR_VALL",
    "VENE": BASE_URL + "INDIR_VENE",
}

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "anncsu-indirizzi.parquet"
PMTILES_FILE = OUTPUT_DIR / "anncsu-indirizzi.pmtiles"
COMUNI_FILE = OUTPUT_DIR / "comuni.json"
BOUNDARIES_FILE = OUTPUT_DIR / "istat-boundaries.parquet"
MARKER_FILE = OUTPUT_DIR / ".last_remote_date"
TAIL_SIZE = 65536
DOWNLOAD_TIMEOUT = 600
MAX_RETRIES = 3
RETRY_WAIT = 30
INTER_DOWNLOAD_SLEEP = 2  # seconds between regional downloads


def _check_tippecanoe() -> None:
    """Raise early if tippecanoe is not installed."""
    import shutil
    if shutil.which("tippecanoe") is None:
        raise SystemExit(
            "tippecanoe not found in PATH. Install it before running:\n"
            "  macOS:  brew install tippecanoe\n"
            "  Ubuntu: sudo apt-get install tippecanoe"
        )
    print("tippecanoe is available")


def _check_server_available() -> None:
    """Raise early if the ANNCSU server is not reachable or returns an error.

    Uses a single-byte GET range request instead of HEAD because the
    Akamai CDN in front of the ANNCSU portal blocks HEAD requests with 403.
    Tries the first regional URL first, then falls back to the national URL.
    """
    probe_urls = [next(iter(REGION_URLS.values())), NATIONAL_URL]
    print("Checking ANNCSU server availability ...")
    last_exc: Exception | None = None
    for probe_url in probe_urls:
        try:
            response = httpx.get(
                probe_url,
                headers={"Range": "bytes=0-0"},
                follow_redirects=True,
                timeout=30,
            )
        except httpx.TransportError as exc:
            last_exc = exc
            continue
        if response.status_code < 400:
            print("Server is available")
            return
        last_exc = Exception(f"HTTP {response.status_code}")
    raise SystemExit(f"ANNCSU server unreachable: {last_exc}")


def get_remote_date() -> datetime | None:
    """Fetch the zip tail of one region to extract the CSV date from its filename."""
    probe_url = next(iter(REGION_URLS.values()))
    print("Checking remote dataset date ...")
    try:
        response = httpx.get(
            probe_url,
            headers={"Range": f"bytes=-{TAIL_SIZE}"},
            follow_redirects=True,
            timeout=60,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        print(f"Warning: could not fetch remote date (HTTP {exc.response.status_code})")
        return None
    except httpx.TransportError as exc:
        print(f"Warning: could not fetch remote date ({exc})")
        return None

    matches = re.findall(rb"INDIR_\w+_(\d{8})\.csv", response.content)
    if not matches:
        print("Could not determine remote date, will proceed with download")
        return None

    date_str = matches[-1].decode()
    remote_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
    print(f"Remote dataset date: {remote_date.date()}")
    return remote_date


def get_local_date() -> datetime | None:
    """Read the last downloaded dataset date from the marker file."""
    if not MARKER_FILE.exists():
        return None

    try:
        date_str = MARKER_FILE.read_text().strip()
        local_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        print(f"Last downloaded dataset date: {local_date.date()}")
        return local_date
    except (ValueError, OSError):
        return None


def save_remote_date(remote_date: datetime) -> None:
    """Persist the remote dataset date to the marker file after a successful download."""
    MARKER_FILE.write_text(remote_date.strftime("%Y%m%d"))


def is_update_needed(remote_date: datetime | None = None) -> bool:
    """Check if the remote dataset is newer than the local one."""
    if remote_date is None:
        remote_date = get_remote_date()
    if remote_date is None:
        return True

    local_date = get_local_date()
    if local_date is None:
        print("No marker file found, will download")
        return True

    if remote_date > local_date:
        print("Remote dataset is newer, will download")
        return True

    print("Local data is up to date, skipping download")
    return False


def _download_region(code: str, url: str) -> Path:
    """Download one regional ZIP and extract its CSV. Returns path to extracted CSV."""
    print(f"[{code}] Downloading ...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = io.BytesIO()
            with httpx.stream(
                "GET", url, follow_redirects=True, timeout=DOWNLOAD_TIMEOUT
            ) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                downloaded = 0
                for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                    data.write(chunk)
                    downloaded += len(chunk)
                    mb = downloaded / (1024 * 1024)
                    if total:
                        pct = downloaded * 100 / total
                        print(f"  [{code}] {mb:.0f} MB / {total / (1024 * 1024):.0f} MB ({pct:.0f}%)")
                    else:
                        print(f"  [{code}] {mb:.0f} MB downloaded ...")
            break
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            if attempt == MAX_RETRIES:
                raise SystemExit(f"[{code}] Download failed after {MAX_RETRIES} attempts: {exc}") from exc
            print(f"  [{code}] Attempt {attempt}/{MAX_RETRIES} failed ({exc}), retrying in {RETRY_WAIT}s ...")
            time.sleep(RETRY_WAIT)

    print(f"  [{code}] Downloaded {downloaded / (1024 * 1024):.1f} MB")

    data.seek(0)
    with zipfile.ZipFile(data) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"[{code}] No CSV found in zip. Contents: {zf.namelist()}")
        csv_name = csv_names[0]
        print(f"  [{code}] Extracting {csv_name} ...")
        zf.extract(csv_name, OUTPUT_DIR)
        return OUTPUT_DIR / csv_name


def download_all_regions() -> list[Path]:
    """Download all regional ZIPs sequentially and return list of extracted CSV paths."""
    csv_paths = []
    regions = list(REGION_URLS.items())
    for i, (code, url) in enumerate(regions):
        csv_path = _download_region(code, url)
        csv_paths.append(csv_path)
        if i < len(regions) - 1:
            time.sleep(INTER_DOWNLOAD_SLEEP)
    print(f"Downloaded {len(csv_paths)} regional CSVs")
    return csv_paths


def csv_to_parquet(csv_paths: list[Path]) -> Path:
    """Load CSVs into DuckDB, join with comuni, create geometries, and export as Parquet."""
    if not COMUNI_FILE.exists():
        raise RuntimeError(
            f"{COMUNI_FILE} not found. Run generate_comuni.py first: "
            "uv run scripts/generate_comuni.py"
        )

    print("Loading CSVs into DuckDB ...")
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{OUTPUT_DIR}'")
    con.execute("INSTALL spatial; LOAD spatial;")

    # Load comuni lookup table from JSON array
    con.execute(f"""
        CREATE TABLE comuni AS
        SELECT codice_istat, nome_comune
        FROM read_json('{COMUNI_FILE}')
    """)

    comuni_count = con.execute("SELECT COUNT(*) FROM comuni").fetchone()[0]
    print(f"Loaded {comuni_count:,} comuni from lookup table")

    # Build quoted list of CSV paths for DuckDB
    csv_list = ", ".join(f"'{p}'" for p in csv_paths)

    con.execute(f"""
        CREATE TABLE addresses AS
        SELECT
            a.CODICE_COMUNE,
            a.CODICE_ISTAT,
            c.nome_comune AS NOME_COMUNE,
            a.PROGRESSIVO_NAZIONALE,
            a.CODICE_COMUNALE,
            a.ODONIMO,
            a.DIZIONE_LINGUA1,
            a.DIZIONE_LINGUA2,
            a.PROGRESSIVO_ACCESSO,
            a.CODICE_COMUNALE_ACCESSO,
            a.CIVICO,
            a.ESPONENTE,
            a.SPECIFICITA,
            a.METRICO,
            a.PROGRESSIVO_SNC,
            CAST(REPLACE(a.COORD_X_COMUNE, ',', '.') AS DOUBLE) AS longitude,
            CAST(REPLACE(a.COORD_Y_COMUNE, ',', '.') AS DOUBLE) AS latitude,
            a.QUOTA,
            a.METODO,
            ST_Point(
                CAST(REPLACE(a.COORD_X_COMUNE, ',', '.') AS DOUBLE),
                CAST(REPLACE(a.COORD_Y_COMUNE, ',', '.') AS DOUBLE)
            ) AS geometry
        FROM read_csv(
            [{csv_list}],
            header=true,
            auto_detect=true,
            ignore_errors=true,
            union_by_name=true
        ) a
        LEFT JOIN comuni c ON a.CODICE_ISTAT = c.codice_istat
        WHERE a.COORD_X_COMUNE IS NOT NULL
          AND a.COORD_Y_COMUNE IS NOT NULL
    """)

    row_count = con.execute("SELECT COUNT(*) FROM addresses").fetchone()[0]
    print(f"Loaded {row_count:,} addresses with coordinates")

    # Flag out-of-bounds addresses using ISTAT municipal boundaries
    if BOUNDARIES_FILE.exists():
        print("Flagging out-of-bounds addresses using ISTAT boundaries ...")
        con.execute(f"""
            CREATE TABLE boundaries AS
            SELECT codice_istat, geometry
            FROM read_parquet('{BOUNDARIES_FILE}')
        """)

        # Threshold in meters: addresses outside their boundary but within
        # this distance are not flagged as out-of-bounds (geocoding tolerance).
        oob_threshold_m = 110

        con.execute(f"""
            CREATE TABLE addresses_validated AS
            SELECT
                a.*,
                CASE
                    WHEN b.geometry IS NULL THEN NULL
                    WHEN ST_Contains(b.geometry, a.geometry) THEN NULL
                    ELSE ROUND(ST_Distance(b.geometry, a.geometry)::DOUBLE * 111000, 2)
                END AS oob_distance_m,
                CASE
                    WHEN b.geometry IS NULL THEN NULL
                    WHEN ST_Contains(b.geometry, a.geometry) THEN false
                    WHEN ST_Distance(b.geometry, a.geometry)::DOUBLE * 111000 > {oob_threshold_m} THEN true
                    ELSE false
                END AS out_of_bounds
            FROM addresses a
            LEFT JOIN boundaries b ON a.CODICE_ISTAT = b.codice_istat
        """)

        con.execute("DROP TABLE addresses")
        con.execute("ALTER TABLE addresses_validated RENAME TO addresses")

        oob_count = con.execute(
            "SELECT COUNT(*) FROM addresses WHERE out_of_bounds = true"
        ).fetchone()[0]
        total = con.execute("SELECT COUNT(*) FROM addresses").fetchone()[0]
        print(f"Flagged {oob_count:,} out-of-bounds addresses out of {total:,}")
    else:
        print(f"Warning: {BOUNDARIES_FILE} not found, skipping out-of-bounds detection")

    # Compute Hilbert sort keys in numpy (only lon/lat needed: ~288 MB),
    # register as Arrow table, then let DuckDB sort+write with external
    # spill to disk. No UDF needed — avoids all DuckDB version issues.
    import numpy as np
    import pyarrow as pa

    print("Computing Hilbert sort keys ...")
    coords = con.execute("SELECT longitude, latitude FROM addresses").fetchnumpy()
    lons = coords["longitude"]
    lats = coords["latitude"]

    n = 1 << 16
    xi = np.clip(((lons + 180.0) / 360.0 * n).astype(np.int64), 0, n - 1)
    yi = np.clip(((lats + 90.0) / 180.0 * n).astype(np.int64), 0, n - 1)
    del lons, lats
    d = np.zeros(len(xi), dtype=np.int64)
    s = n >> 1
    while s > 0:
        rx = ((xi & s) > 0).astype(np.int64)
        ry = ((yi & s) > 0).astype(np.int64)
        d += s * s * ((3 * rx) ^ ry)
        mask = ry == 0
        rot_mask = mask & (rx == 1)
        xi_new = np.where(rot_mask, s - 1 - xi, xi)
        yi_new = np.where(rot_mask, s - 1 - yi, yi)
        xi = np.where(mask, yi_new, xi_new)
        yi = np.where(mask, xi_new, yi_new)
        s >>= 1
    del xi, yi

    # Register Hilbert keys with row indices as an Arrow-backed DuckDB view
    n_rows = len(d)
    hilbert_tbl = pa.table({
        "_rn": np.arange(n_rows, dtype=np.int64),
        "_hilbert_key": d,
    })
    del d
    con.register("_hilbert_keys", hilbert_tbl)

    # Allow DuckDB to spill sort data to disk (avoids OOM on 7 GB runners).
    # con.execute(f"SET temp_directory = '{OUTPUT_DIR}'")

    print(f"Writing Parquet to {OUTPUT_FILE} (Hilbert-sorted) ...")
    con.execute(f"""
        COPY (
            SELECT a.* EXCLUDE (_rn)
            FROM (
                SELECT *, (ROW_NUMBER() OVER ()) - 1 AS _rn
                FROM addresses
            ) a
            JOIN _hilbert_keys h ON a._rn = h._rn
            ORDER BY h._hilbert_key
        ) TO '{OUTPUT_FILE}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    con.close()
    return OUTPUT_FILE


def enhance_with_geoparquet(parquet_path: Path) -> None:
    """Add bbox column using geoparquet-io.

    Hilbert sorting is done inside DuckDB (spills to disk, low memory).
    Only bbox addition remains here.
    """
    import geoparquet_io as gpio

    print("Adding bbox ...")
    gpio.read(str(parquet_path)).add_bbox().write(str(parquet_path))

    print("GeoParquet enhancement complete")


def convert_to_pmtiles(parquet_path: Path) -> Path:
    """Convert GeoParquet to PMTiles for map visualization."""
    from gpio_pmtiles import create_pmtiles_from_geoparquet

    print(f"Converting to PMTiles: {PMTILES_FILE} ...")
    if PMTILES_FILE.exists():
        PMTILES_FILE.unlink()
    create_pmtiles_from_geoparquet(
        str(parquet_path),
        str(PMTILES_FILE),
        layer="addresses",
        include_cols="ODONIMO,CIVICO,ESPONENTE,CODICE_ISTAT,NOME_COMUNE,oob_distance_m,out_of_bounds",
        verbose=True,
    )
    size_mb = PMTILES_FILE.stat().st_size / (1024 * 1024)
    print(f"PMTiles conversion complete ({size_mb:.1f} MB)")
    return PMTILES_FILE


def main(
    force: bool = typer.Option(False, "--force", help="Skip freshness check and force re-download"),
    skip_pmtiles: bool = typer.Option(False, "--skip-pmtiles", help="Skip PMTiles conversion (requires tippecanoe)"),
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not skip_pmtiles:
        _check_tippecanoe()
    _check_server_available()

    remote_date = get_remote_date()

    if not force and not is_update_needed(remote_date):
        return

    try:
        csv_paths = download_all_regions()
    except SystemExit as exc:
        print(f"Regional downloads failed ({exc}), falling back to national file ...")
        csv_paths = [_download_region("ITA", NATIONAL_URL)]

    try:
        parquet_path = csv_to_parquet(csv_paths)
        enhance_with_geoparquet(parquet_path)
        if not skip_pmtiles:
            convert_to_pmtiles(parquet_path)
    finally:
        for csv_path in csv_paths:
            if csv_path.exists():
                csv_path.unlink()
        print("Cleaned up temporary CSVs")

    if remote_date is not None:
        save_remote_date(remote_date)
        print(f"Saved dataset date marker: {remote_date.date()}")

    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"Done! GeoParquet: {parquet_path} ({size_mb:.1f} MB)")
    if PMTILES_FILE.exists():
        pm_size_mb = PMTILES_FILE.stat().st_size / (1024 * 1024)
        print(f"Done! PMTiles: {PMTILES_FILE} ({pm_size_mb:.1f} MB)")


if __name__ == "__main__":
    typer.run(main)
