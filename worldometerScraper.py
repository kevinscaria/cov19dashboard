import pandas as pd
from datetime import datetime, timedelta
from gazpacho import Soup

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
world['date'] = today.strftime('%d-%m-%Y')

#Save daily data
world.to_csv('data/worldometerData.csv', index = False)

#Date Management
today = datetime.today()

#Load data dump
df = pd.read_csv('data/worldometerTimeSeriesData.csv')

#Append new data
df = df.append(world)
df.to_csv('data/worldometerTimeSeriesData.csv', index = False)
