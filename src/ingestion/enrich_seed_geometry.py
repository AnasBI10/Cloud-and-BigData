
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request

ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
SEED_PATH = pathlib.Path("data/dot_links_seed.json")

FETCH_LIMIT = 5000

# NYC liegt zwischen rund 40.4..41.0 N und -74.3..-73.6 E. Punkte ausserhalb sind Datenfehler des Feeds.
LAT_RANGE = (40.3, 41.1)
LON_RANGE = (-74.4, -73.5)


def fetch_geometry(app_token: str | None) -> dict[str, str]:
    """Je link_id die erste brauchbare Polylinie."""
    query = urllib.parse.urlencode(
        {"$select": "link_id,link_points", "$limit": FETCH_LIMIT}
    )
    request = urllib.request.Request(f"{ENDPOINT}?{query}")
    if app_token:
        request.add_header("X-App-Token", app_token)

    with urllib.request.urlopen(request, timeout=60) as response:
        rows = json.load(response)

    geometry: dict[str, str] = {}
    for row in rows:
        link_id = row.get("link_id")
        points = (row.get("link_points") or "").strip()
        if link_id and points and link_id not in geometry:
            geometry[link_id] = points
    return geometry


def clean(points: str) -> str | None:
    """Verwirft abgeschnittene Koordinatenpaare und Ausreisser ausserhalb NYC."""
    kept = []
    for pair in points.split():
        lat_str, _, lon_str = pair.partition(",")
        try:
            lat, lon = float(lat_str), float(lon_str)
        except ValueError:
            continue
        if LAT_RANGE[0] <= lat <= LAT_RANGE[1] and LON_RANGE[0] <= lon <= LON_RANGE[1]:
            kept.append(f"{lat:.6f},{lon:.6f}")
    return " ".join(kept) if len(kept) >= 2 else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="nichts schreiben")
    args = parser.parse_args()

    if not SEED_PATH.exists():
        print(f"{SEED_PATH} nicht gefunden — im Repo-Wurzelverzeichnis aufrufen.")
        return 2

    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    geometry = fetch_geometry(os.getenv("SOCRATA_APP_TOKEN"))
    print(f"Feed: Geometrie fuer {len(geometry)} link_id abgerufen")

    updated = skipped = unchanged = 0
    for segment in seed:
        points = geometry.get(segment["link_id"])
        cleaned = clean(points) if points else None
        if cleaned is None:
            skipped += 1
            continue
        if segment.get("link_points") == cleaned:
            unchanged += 1
            continue
        segment["link_points"] = cleaned
        updated += 1

    print(f"ergaenzt: {updated} | unveraendert: {unchanged} | ohne Geometrie: {skipped}")
    if skipped:
        missing = [s["link_id"] for s in seed if not s.get("link_points")]
        print("ohne Geometrie:", ", ".join(missing))

    if args.dry_run:
        print("--dry-run: nichts geschrieben")
        return 0

    SEED_PATH.write_text(
        json.dumps(seed, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"{SEED_PATH} geschrieben ({SEED_PATH.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
