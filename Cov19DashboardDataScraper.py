import pandas as pd
import requests, jinja2, sys, smtplib, warnings
from email.message import EmailMessage
warnings.filterwarnings('ignore')
from datetime import datetime, timedelta
sys.path.append(PATH + "assets/reports/")

class Error(Exception):
    """Base class for python exceptions"""
    pass

class ColumnMismatchError(Error):
    """Raised when the column names have changed from previous data dump"""
    pass

#Main Data India
try:
    temp = pd.read_csv(PATH + "assets/data/vaxDf.csv")
    vaxDf = pd.read_csv('http://api.covid19india.org/csv/latest/cowin_vaccine_data_districtwise.csv', dtype=object)
    print('District level vax data fetched from url')
    vaxDf = vaxDf[vaxDf['State'].notnull()]
    if set(temp.columns).issubset(set(vaxDf.columns[:len(temp.columns)])):
        vaxDf.to_csv(PATH + "assets/data/vaxDf.csv", index=False)
        print('Saving district level vaccination data at location {}'.format(PATH + "assets/data/vaxDf.csv"))
        dataloaddict['vaxDf'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['vaxDf'] = False
    print('Column mismatch in district level vaccination data. Using last succesful data dump.')
    vaxDf = pd.read_csv(PATH + "assets/data/vaxDf.csv")

try:
    temp = pd.read_csv(PATH + "assets/data/vaxDfS.csv")
    vaxDfS = pd.read_csv('http://api.covid19india.org/csv/latest/cowin_vaccine_data_statewise.csv')
    print('State level vax data fetched from url')
    vaxDfS = vaxDfS[vaxDfS['State'] != 'India']
    if set(temp.columns).issubset(set(vaxDfS.columns)):
        vaxDfS.to_csv(PATH + "assets/data/vaxDfS.csv", index=False)
        print('Saving state level vaccination data at location {}'.format(PATH + "assets/data/vaxDfS.csv"))
        dataloaddict['vaxDfS'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['vaxDfS'] = False
    print('Column mismatch in state level vaccination data. Using last succesful data dump.')
    vaxDfS = pd.read_csv(PATH + "assets/data/vaxDfS.csv")


try:
    temp = pd.read_csv(PATH + "assets/data/indDf.csv")
    indDf = pd.read_csv('https://api.covid19india.org/csv/latest/districts.csv')
    print('District level main data fetched from url')
    indDf = indDf[indDf['District']!= 'Unknown']
    finalRem = ['State Pool', 'Airport Quarantine', 'Gaurela Pendra Marwahi',
				 'Other State', 'Dakshin Bastar Dantewada', 'Pakke Kessang',
				 'Chhota Udaipur', 'Railway Quarantine', 'Prayagraj',
				 'Kalaburagi', 'Others', 'Italians',
				 'Y.S.R. Kadapa', 'Foreign Evacuees', 'Capital Complex',
				 'Evacuees', 'Uttar Bastar Kanker', 'Ayodhya', 'BSF Camp', 'Other Region']
    indDf = indDf[~indDf['District'].isin(finalRem)]
    if set(temp.columns).issubset(set(indDf.columns)):
        indDf.to_csv(PATH + "assets/data/indDf.csv", index=False)
        print('Saving district level main data at location {}'.format(PATH + "assets/data/indDf.csv"))
        dataloaddict['indDf'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['indDf'] = False
    print('Column mismatch in district level main data. Using last succesful data dump.')
    indDf = pd.read_csv(PATH + "assets/data/indDf.csv")

try:
    temp = pd.read_csv(PATH + "assets/data/indDfS.csv")
    indDfS = pd.read_csv('https://api.covid19india.org/csv/latest/states.csv')
    print('State level main data fetched from url')
    indDfS = indDfS[(indDfS['State'] != 'India') & (indDfS['State'] != 'State Unassigned')]
    if set(temp.columns).issubset(set(indDfS.columns)):
        indDfS.to_csv(PATH + "assets/data/indDfS.csv", index=False)
        print('Saving state level main data at location {}'.format(PATH + "assets/data/indDfS.csv"))
        dataloaddict['indDfS'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['indDfS'] = False
    print('Column mismatch in state level main data. Using last succesful data dump.')
    indDfS = pd.read_csv(PATH + "assets/data/indDfS.csv")


#Main Data Global
try:
    temp = pd.read_csv(PATH + "assets/data/vax_by_man.csv")
    vax_by_man = pd.read_csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations-by-manufacturer.csv')
    print('Country level vaccine manufacturer data fetched from url')
    if set(temp.columns).issubset(set(vax_by_man.columns)):
        vax_by_man.to_csv(PATH + "assets/data/vax_by_man.csv", index=False)
        print('Saving country level vaccine manifacturer data at {}'.format(PATH + "assets/data/vax_by_man.csv"))
        dataloaddict['vax_by_man'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['vax_by_man'] = False
    print('Column mismatch in country level vaccine manufacturer data. Using last succesful data dump.')
    vax_by_man = pd.read_csv(PATH + "assets/data/vax_by_man.csv")

try:
    temp = pd.read_csv(PATH + "assets/data/mx.csv")
    mx = pd.read_csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv')
    print('Country level main data fetched from url')
    mx = mx[mx['continent'].notnull()]
    vax_by_man = pd.read_csv(PATH + "assets/data/vax_by_man.csv")
    print('Country level vaccination manufacturer loaded')
    vaxMan = pd.pivot_table(vax_by_man, values='total_vaccinations', index=['location', 'date'],
	                    columns=['vaccine'], aggfunc=np.sum).reset_index()
    vaxMan.columns.name = None
    mx = pd.merge(mx, vaxMan, on = ['date', 'location'], how = 'left')
    indVax = vaxDfS.groupby('Updated On').sum()[[' Covaxin (Doses Administered)', 'CoviShield (Doses Administered)',
	       'Sputnik V (Doses Administered)']].reset_index().rename(columns = {'Updated On':'date'})
    indVax['location'] = 'India'
    indVax['date'] = pd.to_datetime(indVax['date'], format='%d/%m/%Y')
    mx['date'] = pd.to_datetime(mx['date'])
    mx = pd.merge(mx, indVax, on = ['date', 'location'], how = 'left')
    print('Country level main data joined with vaccine manufacturer data.')
    if set(temp.columns).issubset(set(mx.columns)):
        mx.to_csv(PATH + "assets/data/mx.csv", index=False)
        print('Saving final country level data at location {}'.format(PATH + "assets/data/mx.csv"))
        dataloaddict['mx'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['mx'] = False
    print('Column mismatch in country level main data. Using last succesful data dump.')
    mx = pd.read_csv(PATH + "assets/data/mx.csv")


#Mobility Data
try:
    temp = pd.read_pickle(PATH + "assets/data/Mobility.bz2")
    url_countries = "https://raw.githubusercontent.com/ActiveConclusion/COVID19_mobility/master/google_reports/mobility_report_countries.csv"
    mobility = pd.read_csv(url_countries)
    print('Mobility global data fetched from url')
    mobility = mobility[mobility['region'] != 'Total']
    mobility.columns = ['Country', 'Region', 'date', 'retail_and_recreation_percent_change_from_baseline',
                            'grocery_and_pharmacy_percent_change_from_baseline',
                            'parks_percent_change_from_baseline',
                            'transit_stations_percent_change_from_baseline',
                            'workplaces_percent_change_from_baseline',
                            'residential_percent_change_from_baseline']
    if set(temp.columns).issubset(set(mobility.columns)):
        mobility.to_pickle(PATH + "assets/data/Mobility.bz2", compression = 'bz2')
        print('Saving new compressed country level mobility data to location {}'.format(PATH + "assets/data/Mobility.bz2"))
        dataloaddict['mobility'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['mobility'] = False
    print('Column mismatch in mobility global data. Loading from data dump.')
    mobility = pd.read_pickle(PATH + "assets/data/Mobility.bz2")

try:
    temp = pd.read_pickle(PATH + "assets/data/MobilityInd.bz2")
    url_asia = "https://raw.githubusercontent.com/ActiveConclusion/COVID19_mobility/master/google_reports/mobility_report_asia_africa.csv"
    mobilityInd = pd.read_csv(url_asia)
    print('Mobility India data fetched from url')
    mobilityInd = mobilityInd[mobilityInd['country'] == "India"]
    mobilityInd.loc[mobilityInd['sub region 1'] == 'Dadra and Nagar Haveli', 'sub region 1'] = 'Dadra and Nagar Haveli and Daman and Diu'
    mobilityInd.loc[mobilityInd['sub region 1'] == 'Daman and Diu', 'sub region 1'] = 'Dadra and Nagar Haveli and Daman and Diu'
    mobilityInd = mobilityInd[mobilityInd['sub region 1'] != 'Total']
    mobilityInd = mobilityInd.iloc[:, 2:]
    mobilityInd.columns = ['State', 'District', 'date', 'retail_and_recreation_percent_change_from_baseline',
                            'grocery_and_pharmacy_percent_change_from_baseline',
                            'parks_percent_change_from_baseline',
                            'transit_stations_percent_change_from_baseline',
                            'workplaces_percent_change_from_baseline',
                            'residential_percent_change_from_baseline']
    if set(temp.columns).issubset(set(mobilityInd.columns)):
        mobilityInd.to_pickle(PATH + "assets/data/MobilityInd.bz2", compression = 'bz2')
        print('Saving new compressed India level mobility data to location {}'.format(PATH + "assets/data/MobilityInd.bz2"))
        dataloaddict['mobilityInd'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['mobilityInd'] = False
    print('Column mismatch in mobility india data. Loading from data dump.')
    mobilityInd = pd.read_pickle(PATH + "assets/data/MobilityInd.bz2")


#Variants Data
try:
    temp = pd.read_csv(PATH + "assets/data/variantsDf.csv")
    per_country_json = requests.get('https://github.com/hodcroftlab/covariants/raw/master/cluster_tables/EUClusters_data.json').json()
    variantsDf = pd.DataFrame()
    for country in per_country_json['countries'].keys():
        temp_df = pd.DataFrame.from_dict(per_country_json['countries'][country] , orient = 'columns')
        temp_df['country'] = country
        temp_df.loc[: , 'eow_dt'] = temp_df.week.astype('datetime64') + pd.offsets.Week(weekday=6)
        nextstrain_clades = list(per_country_json['countries'][country].keys())[2:]
        temp_df.loc[: , nextstrain_clades ] = temp_df[nextstrain_clades].div(temp_df.total_sequences , axis=0) * 100
        variantsDf = variantsDf.append(temp_df , ignore_index=True)
    print('Variants global data fetched from url')
    variantsDf['country'] = variantsDf['country'].replace('USA', 'United States').replace('Czech Republic', 'Czechia').replace('Sint Maarten', 'Sint Maarten (Dutch part)').replace('Bonaire', 'Bonaire Sint Eustatius and Saba')
    variantsDf.rename(columns = dict(zip(['20H (Beta, V2)', '20I (Alpha, V1)', '20J (Gamma, V3)', '21A (Delta)',
    '21B (Kappa)', '21C (Epsilon)', '21D (Eta)', '21F (Iota)'],
                                            [i.split('(')[1].split(',')[0].replace(')', '') for i in ['20H (Beta, V2)', '20I (Alpha, V1)', '20J (Gamma, V3)', '21A (Delta)',
    '21B (Kappa)', '21C (Epsilon)', '21D (Eta)', '21F (Iota)']])) , inplace=True)
    if set(temp.columns).issubset(set(variantsDf.columns)):
        variantsDf.to_csv(PATH + "assets/data/variantsDf.csv", index=False)
        print('Saving country level variants data at location {}'.format(PATH + "assets/data/variantsDf.csv"))
        dataloaddict['variantsDf'] = True
    else:
        raise ColumnMismatchError
except:
    dataloaddict['variantsDf'] = False
    print('Column mismatch in country level variants data. Using last succesful data dump.')


#ColumnMismatchError Notifier
mismatchData = []
for key, val in dataloaddict.items():
    if val == False:
        mismatchData.append(key)
print('Number of mismatch columns: ', len(mismatchData))

if len(mismatchData) > 0:
    try:
        msg = EmailMessage()
        msg.set_content("""\
            Hi, \n
            Column mismatch in the following datasets: \n
            {}""".format(', '.join(mismatchData)))
        msg['Subject'] = 'ColumnMismatchError'
        msg['From'] = "tigeriitmcov19relief@gmail.com"
        msg['To'] = "kevin.scaria@tigeranalytics.com"
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login("tigeriitmcov19relief@gmail.com", "tigeriitmsecret")
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(e)
