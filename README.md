# data-gov-gr
A python script to download all datasets from [https://data.gov.gr](https://data.gov.gr). It will scrape open data from
the API.

# Fetch
[Fetch](src/fetch.py) is the main script of this repo. It downloads all data from all API endpoints in data.gov.gr

## How it works
In [`fetch.py`](src/fetch.py) you can see all endpoints we scrape data from. For each endpoint we optionally set a
start date. If this is set, then we use `date_from` `date_to` params to the API query to ask data for each day
individually. We do this to reduce the load of the API and avoid getting banned. The rest endpoints are queried without
a date range. We store the results as uncompressed json files on the output directory defined as script argument. 
To run incrementally, one can comment out the endpoints in `fetch.py` or change the start date.

## Environment Variables
One needs to export the following variables
* `DATA_GOV_GR_TOKEN` containing the token. You can get a token [here](https://data.gov.gr/token)
You can export it, or create a file called `env` in the root of the project and write it there like `DATA_GOV_GR_TOKEN=xxx`

## How to run in Python
* Check your python is >=3.8 `python3 --version`
* Create a virtual environment `python3 -m venv venv/`
* Install all run dependencies `pip install -r requirements.txt`
* Export token `export DATA_GOV_GR_TOKEN=xxx`
* Run `python src/fetch.py --data /data/path`

## Error handling
So far there is no documentation on the api, describing the limits. Therefore, we let the script run, and we retry in 
case of `429-Too Many Requests` error. We also retry on `504-Request Timeout`. 

# Process
[Process](src/process.py) is a secondary script which reads all generated json, merges in one csv (per endpoint). This
can be useful for opening the data in Excel or having an easier overview. It also reduces the size. This data is also
committed in this repo, so you can directly download the files.

## How it works
We read all endpoint folders created by [Fetch](#fetch) and for each of them, we read all produced jsons and merge them
in one csv.

## How to run in Python
Assuming you have run already [fetch](#fetch)
* Run `python src/process.py --data /data/path`
