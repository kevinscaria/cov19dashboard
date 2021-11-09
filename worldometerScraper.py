import pandas as pd
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
vaxDf = vaxDf[vaxDf['State'].notnull()]
vaxDf.to_csv("data/vaxDf.csv", index=False)

#Download state wise vax data
vaxDfS = pd.read_csv('https://data.incovid19.org/csv/latest/cowin_vaccine_data_statewise.csv')
vaxDfS = vaxDfS[vaxDfS['State'] != 'India']
vaxDfS.to_csv("data/vaxDfS.csv", index=False)

#Download india district wise main data
indDf = pd.read_csv('	https://data.incovid19.org/csv/latest/districts.csv')
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


##########################################################################################################


#Data from India MOHFW - Statewise Cases
soup = Soup.get('https://www.mygov.in/corona-data/covid19-statewise-status')

val_list = {}
for idx, field_item in enumerate(soup.find("div", {"class": "field-items"}, mode="all")):
    val_list[idx] = field_item.find("div", {"class": "field-item even"}, mode="all")

state_dict = {}

#Indirect Cases
for tgs in val_list[15]:
    for child in tgs.find("div", {"class": "field-item even"}, mode="all"):
        try:
            subdiv = child.find("div", {"class": "field-collection-view clearfix view-mode-full"}, mode="first")
            subdiv = subdiv.find("div", {"class": "entity entity-field-collection-item field-collection-item-field-covid-statewise-data clearfix"}, mode="first")
            subdiv = subdiv.find("div", {"class": "content"}, mode="first")
            state = subdiv.find("div", {"class": "field field-name-field-select-state field-type-list-text field-label-above"}, mode="first")
            state = state.find("div", {"class": "field-items"}, mode="first")
            state = state.find("div", {"class": "field-item even"}, mode="first").text

            state_dict[state] = []
            state_dict[state].append(today)

            confirmed = subdiv.find("div", {"class": "field field-name-field-total-confirmed-indians field-type-number-integer field-label-above"}, mode="first")
            confirmed = confirmed.find("div", {"class": "field-items"}, mode="first")
            confirmed = int(confirmed.find("div", {"class": "field-item even"}, mode="first").text)
            state_dict[state].append(confirmed)

            cured = subdiv.find("div", {"class": "field field-name-field-cured field-type-number-integer field-label-above"}, mode="first")
            cured = cured.find("div", {"class": "field-items"}, mode="first")
            cured = int(cured.find("div", {"class": "field-item even"}, mode="first").text)
            state_dict[state].append(cured)

            deaths = subdiv.find("div", {"class": "field field-name-field-deaths field-type-number-integer field-label-above"}, mode="first")
            deaths = deaths.find("div", {"class": "field-items"}, mode="first")
            deaths = int(deaths.find("div", {"class": "field-item even"}, mode="first").text)
            state_dict[state].append(deaths)
        except:
            pass

#Direct Cases
direct_cases = soup.find("div", {"class": "field-item even"}, mode="all")
for idx, i in enumerate(direct_cases):
    if i.text == 'State Name:':
        state_dict[direct_cases[idx+1].text] = [today] + [int(j.text) for j in direct_cases[idx+2:idx+5]]

indDfs_mohfw = pd.DataFrame(state_dict, index = ['date', 'Confirmed', 'Recovered', 'Death']).T.reset_index()
indDfs_mohfw = indDfs_mohfw.rename(columns = {'index':'State'})
indDfs_mohfw.to_csv("data/indDfS_MOHFW.csv", index=False)

#Load data dump
mohfwdf = pd.read_csv('data/indDfS_MOHFW.csv')

#Append new data
mohfwdf = mohfwdf.append(indDfs_mohfw)
mohfwdf = mohfwdf.drop_duplicates(subset = ['date', 'State'])
mohfwdf.to_csv('data/indDfS_MOHFW.csv', index = False)
