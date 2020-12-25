import argparse
import csv
import json
import logging
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main(data_dir: Path):
    for folder in data_dir.glob("*"):
        if not folder.is_dir():
            continue

        logging.info(f"Processing {folder.name}...")
        csv_file = folder / f"{folder.name}.csv"
        data = []
        for json_file in sorted(folder.rglob("*.json")):
            logging.info(f"Reading {json_file}...")
            with open(json_file) as json_in:
                for line in json_in:
                    data.append(json.loads(line))
        with open(csv_file, "w") as csv_out:
            writer = csv.DictWriter(
                csv_out,
                dialect="unix",
                quoting=csv.QUOTE_MINIMAL,
                fieldnames=sorted(set(k for d in data for k in d.keys())),
            )
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        logging.info(f"Successfully wrote {csv_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process all downloaded datasets.",
        usage="python process.py --data data/directory/path",
    )
    parser.add_argument(
        "--data",
        dest="data",
        type=Path,
        help="The folder where all data should be stored",
    )
    args = parser.parse_args()

    main(args.data)
