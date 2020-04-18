# luigi webscraping pipeline
Run this script to download benchmarking publically available data via NHS England.

## How it works
Each month NHS England publishes data that every NHS Hospital in the UK submits for the key target indicators for 
Admissions, Emergency Departments and Cancer.

Running this pipeline will scrape the web to download all available files, then transform them into usable data.

## Installing dependencies
```sh
pip3 install -r requirements.txt
```