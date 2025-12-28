import json
from collections import defaultdict

INPUT_FILE = "./data/spotify-global-music-dataset-10000.json"
OUTPUT = "./data/partition_key_output.txt"

def analyze_partition_keys(file_path):
    artists = set()
    by_artist = defaultdict(int)
    by_year = defaultdict(int)
    by_year_month = defaultdict(int)

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

        for track in data:
            artist = track.get("artist_name")
            if artist:
                artists.add(artist)
                by_artist[artist] += 1

            release_date = track.get("album_release_date")
            if release_date and len(release_date) >= 7:
                year = release_date[:4]
                year_month = release_date[:7]
                by_year[year] += 1
                by_year_month[year_month] += 1

    return {
        "distinct_artists": len(artists),
        "distribution_by_artist": dict(by_artist),
        "distribution_by_year": dict(by_year),
        "distribution_by_year_month": dict(by_year_month),
    }

if __name__ == "__main__":
    result = analyze_partition_keys(INPUT_FILE)

    with open(OUTPUT, "w", encoding="utf-8") as out:
        out.write(f"ARTISTAS_DISTINTOS={result['distinct_artists']}\n\n")

        out.write("## DISTRIBUCIÓN POR ARTISTA\n")
        for artist, count in sorted(result["distribution_by_artist"].items()):
            out.write(f"    {artist}: {count}\n")

        out.write("\n## DISTRIBUCIÓN POR AÑO\n")
        for year, count in sorted(result["distribution_by_year"].items()):
            out.write(f"    {year}: {count}\n")

        out.write("\n## DISTRIBUCIÓN POR AÑO-MES\n")
        for ym, count in sorted(result["distribution_by_year_month"].items()):
            out.write(f"    {ym}: {count}\n")
    print(f"Análisis completado. Resultados guardados en {OUTPUT}")