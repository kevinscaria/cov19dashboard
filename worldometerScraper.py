import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from gazpacho import Soup

#Date Management
today = datetime.today().strftime('%d-%m-%Y')

# Download Data to system
soup = Soup.get('https://www.worldometers.info/coronavirus/')
table = soup.find("table", {"id": "main_table_countries_today"}, mode="first")
trs = table.find("tr")[9:-8]
dct = {}
for tr in trs:
    dct[tr.find("td")[1].text] = {}
    dct[tr.find("td")[1].text]['total_cases'] = tr.find("td")[2].text.replace(',', '')
    dct[tr.find("td")[1].text]['total_deaths'] = tr.find("td")[4].text.replace(',', '')
    dct[tr.find("td")[1].text]['total_recovered'] = tr.find("td")[6].text.replace(',', '')
    dct[tr.find("td")[1].text]['active_cases'] = tr.find("td")[8].text.replace(',', '')

world = pd.DataFrame(dct).T

world['total_cases'] = world['total_cases'].apply(lambda x: int(x) if ((x != 'N/A') and (x != '')) else 0)
world['total_deaths'] = world['total_deaths'].apply(lambda x: int(x) if ((x != 'N/A') and (x != '')) else 0)
world['total_recovered'] = world['total_recovered'].apply(lambda x: int(x) if ((x != 'N/A') and (x != '')) else 0)
world['active_cases'] = world['active_cases'].apply(lambda x: int(x) if ((x != 'N/A') and (x != '')) else 0)
world = world.reset_index().rename(columns = {'index':'location'})
world.loc[world['location'] == 'USA', 'location'] = 'United States'
world.loc[world['location'] == 'UK', 'location'] = 'United Kingdom'
world.loc[world['location'] == 'The Bahamas', 'location'] = 'Bahamas'
world.loc[world['location'] == 'Cabo Verde', 'location'] = 'Cape Verde'
world['date'] = today

#Save daily data
world.to_csv('data/worldometerData.csv', index = False)

#Load data dump
df = pd.read_csv('data/worldometerTimeSeriesData.csv')

#Append new data
df = df.append(world)
df.to_csv('data/worldometerTimeSeriesData.csv', index = False)

#Download district wise vax data
vaxDf = pd.read_csv('https://data.incovid19.org/csv/latest/cowin_vaccine_data_districtwise.csv', dtype=object)
new_col_names = np.array(vaxDf.columns) + '._' + np.array(vaxDf.iloc[0, :].values)
vaxDf.columns = new_col_names
vaxDf = vaxDf[vaxDf['State._ '].notnull()]
vaxDf.to_csv("data/vaxDf.csv", index=False)

#Download state wise vax data
vaxDfS = pd.read_csv('https://data.incovid19.org/csv/latest/cowin_vaccine_data_statewise.csv')
vaxDfS = vaxDfS[vaxDfS['State'] != 'India']
vaxDfS.to_csv("data/vaxDfS.csv", index=False)

#Download india district wise main data
indDf = pd.read_csv('https://data.incovid19.org/csv/latest/districts.csv')
indDf = indDf[indDf['District']!= 'Unknown']
finalRem = ['State Pool', 'Airport Quarantine', 'Gaurela Pendra Marwahi',
			 'Other State', 'Dakshin Bastar Dantewada', 'Pakke Kessang',
			 'Chhota Udaipur', 'Railway Quarantine', 'Prayagraj',
			 'Kalaburagi', 'Others', 'Italians',
			 'Y.S.R. Kadapa', 'Foreign Evacuees', 'Capital Complex',
			 'Evacuees', 'Uttar Bastar Kanker', 'Ayodhya', 'BSF Camp', 'Other Region']
indDf = indDf[~indDf['District'].isin(finalRem)]
indDf.to_csv("data/indDf.csv", index=False)

#Download india state wise main data
indDfS = pd.read_csv('https://data.incovid19.org/csv/latest/states.csv')
indDfS = indDfS[(indDfS['State'] != 'India') & (indDfS['State'] != 'State Unassigned')]
indDfS.to_csv("data/indDfS.csv", index=False)
