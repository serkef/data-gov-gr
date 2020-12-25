import argparse
import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional, List
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv, find_dotenv
from requests import HTTPError
from retry import retry

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

endpoints = [
    {"endpoint": "admie_dailyenergybalanceanalysis", "date_from": "2020-11-30"},
    {"endpoint": "admie_realtimescadares", "date_from": "2020-11-30"},
    {"endpoint": "admie_realtimescadasystemload", "date_from": "2020-11-30"},
    {"endpoint": "cadastre_natura_plot", "date_from": "2020-11-02"},
    {"endpoint": "cadastre_plot", "date_from": "2020-11-02"},
    {"endpoint": "eeep_casino_tickets"},
    {"endpoint": "eett_telecom_indicators"},
    {"endpoint": "efet_inspections"},
    {"endpoint": "electricity_consumption", "date_from": "2020-08-09"},
    {"endpoint": "elte_auditors"},
    {"endpoint": "grnet_atlas"},
    {"endpoint": "grnet_eudoxus"},
    {"endpoint": "hcg_incidents"},
    {"endpoint": "internet_traffic", "date_from": "2019-10-18"},
    {"endpoint": "mcp_crime"},
    {"endpoint": "mcp_financial_crimes"},
    {"endpoint": "mcp_forest_fires", "date_from": "2000-01-01"},
    {"endpoint": "mcp_traffic_accidents"},
    {"endpoint": "mcp_traffic_violations"},
    {"endpoint": "mcp_urban_incidents", "date_from": "2014-01-01"},
    {"endpoint": "mindev_realtors"},
    {"endpoint": "minedu_dep"},
    {"endpoint": "minedu_students_school"},
    {"endpoint": "minenv_inspectors"},
    {"endpoint": "minhealth_dentists"},
    {"endpoint": "minhealth_doctors"},
    {"endpoint": "minhealth_pharmacies"},
    {"endpoint": "minhealth_pharmacists"},
    {"endpoint": "minint_election_age"},
    {"endpoint": "minint_election_distribution"},
    {"endpoint": "minjust_law_firms"},
    {"endpoint": "minjust_lawyers"},
    {"endpoint": "mintour_agencies"},
    {"endpoint": "oaed_unemployment", "date_from": "2010-02-01"},
    {"endpoint": "oasa_ridership", "date_from": "2020-08-08"},
    {"endpoint": "oee_accountants"},
    {"endpoint": "road_traffic_attica", "date_from": "2020-11-05"},
    {"endpoint": "sailing_traffic", "date_from": "2017-01-03"},
]


class RetryException(Exception):
    pass


class Fetcher:
    BASE_URL = "https://data.gov.gr/api/v1/"
    RETRY_ARGS = {"tries": 1_000, "delay": 60, "exceptions": RetryException}

    def __init__(
        self,
        object_: str,
        from_date: Optional[dt.date],
        to_date: Optional[dt.date],
        filepath: Path,
    ):
        self.output_file = filepath
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.access_token = os.environ["DATA_GOV_GR_TOKEN"]
        self.object_ = object_
        self.from_date = from_date
        self.to_date = to_date
        logging.info(f"Will fetch {object_}, from:{from_date} to:{to_date}")

    @property
    def params(self) -> Optional[Dict]:
        if not self.from_date and not self.to_date:
            return None
        return {
            "date_from": self.from_date.isoformat(),
            "date_to": self.to_date.isoformat(),
        }

    @property
    def headers(self):
        return {
            "Authorization": "Token " + self.access_token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get_endpoint_for(self, object_: str, *args) -> str:
        path_parts = (object_, *args)
        return urljoin(self.BASE_URL, "/".join(path_parts))

    @retry(**RETRY_ARGS)
    def query(self) -> List[Dict]:
        logging.info(f"Querying {self.object_}...")
        response = requests.get(
            url=self.get_endpoint_for("query", self.object_),
            headers=self.headers,
            params=self.params,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            if response.status_code in {429, 504}:
                raise RetryException(exc)

        return json.loads(response.content)

    def sink(self, data: List[Dict]) -> None:
        if not data:
            logging.warning("No data")
            return

        logging.info(f"Writing to file {self.output_file}...")
        with open(self.output_file, "w") as out:
            for dictionary in data:
                json.dump(dictionary, out, ensure_ascii=False, sort_keys=True)
                out.write("\n")

    def fetch(self):
        self.sink(self.query())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download all datasets from data.gov.gr",
        usage="python fetch.py --data data/directory/path",
    )
    parser.add_argument(
        "--data",
        dest="data",
        type=Path,
        help="The folder where all data should be stored",
    )
    args = parser.parse_args()

    for endpoint in endpoints:
        obj = endpoint["endpoint"]

        # Run without date params
        if "date_from" not in endpoint:
            file = args.data / obj / f"{obj}.json"
            fetcher = Fetcher(obj, None, None, file)
            fetcher.fetch()
            continue

        # Run with date params
        date = dt.date.fromisoformat(endpoint["date_from"])
        while date < dt.date.today():
            file = args.data / obj / date.isoformat() / f"{obj}.json"
            fetcher = Fetcher(obj, date, date, file)
            fetcher.fetch()
            date += dt.timedelta(days=1)
