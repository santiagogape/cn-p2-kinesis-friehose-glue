import csv
import json

INPUT_CSV = "./archive/spotify_data clean.csv"
OUTPUT_JSON = "./data/spotify-global-music-dataset-10000.json"
MAX_ROWS = 10000

def csv_to_json(input_csv, output_json, max_rows):
    records = []

    with open(input_csv, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            records.append(row)

    with open(output_json, "w", encoding="utf-8") as jsonfile:
        json.dump(records, jsonfile, ensure_ascii=False, indent=2)

    print(f"Convertidas {len(records)} filas a JSON")

if __name__ == "__main__":
    csv_to_json(INPUT_CSV, OUTPUT_JSON, MAX_ROWS)
