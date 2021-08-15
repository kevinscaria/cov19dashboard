[![scraper-worldometer](https://github.com/kevin-scaria/WorldometerCOVIDTableScraper/actions/workflows/scraperWorldometer.yml/badge.svg)](https://github.com/kevin-scaria/WorldometerCOVIDTableScraper/actions/workflows/scraperWorldometer.yml)

# COVID-19 Data Scraper

This repository runs daily script to scrape the required COVID 19 information at a global and Indian states and districts level from various sources. The motivation behind this project is bypass the proxy restrictions by several free hosting services such as pythonanywhere, which inhibits fetching data directly from the API that is not whitelisted by them.

The sources from which data is scraped daily is as follows:
1) Worldometer Coronavirus update (Link: https://www.worldometers.info/coronavirus/) - Country wise active cases, 
2) COVID19-India API (https://data.covid19india.org/) - Statewise and districtwise data

The data can be used for further analysis or to integrate it in your projects.
