[![scraper-worldometer](https://github.com/kevin-scaria/WorldometerCOVIDTableScraper/actions/workflows/scraperWorldometer.yml/badge.svg)](https://github.com/kevin-scaria/WorldometerCOVIDTableScraper/actions/workflows/scraperWorldometer.yml)

# COVID-19 Data Scraper

This repository runs daily script to scrape the required COVID 19 information at a global, Indian states and districts level from various sources. The motivation behind this project is bypass the proxy restrictions by several free hosting services such as pythonanywhere, which inhibits fetching data directly from the API that is not whitelisted by them.

The sources from which data is scraped daily is as follows:
1) Worldometer Coronavirus update (Link: https://www.worldometers.info/coronavirus/) - Country wise active cases, total cases, total deaths, total recovered and active cases as a time series. 
2) COVID19-India API (https://data.covid19india.org/) - Statewise and districtwise cases, vaccination and testing data.

The data can be used for further analysis or to integrate it in your projects.

The following data is used along with other open source data to power the Cov19Dashboard which is live at the following link:

URL to the web application: http://cov19dashboard.pythonanywhere.com/
