# -*- coding: utf-8 -*-
"""
Created on Sun Dec  6 03:16:22 2020

@author: TanBW
"""
# these packages need to be install first
import pandas as pd 

import progressbar
import pprint
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import plotly.figure_factory as ff
import numpy as np

import os
import glob

ua = UserAgent()
header = {'User-Agent':str(ua.chrome)}

DEFAULT_TIMEOUT = 5 # seconds

class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


s = requests.Session() 



retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
s.mount("http://", TimeoutHTTPAdapter(max_retries=retries))
s.mount("https://", TimeoutHTTPAdapter(max_retries=retries))


# convert excel file to csv#
read_file = pd.read_excel('D:\\data\Water Fluoridation\\zip\\uszips.xlsx')
read_file.to_csv (r'D:\data\Water Fluoridation\zip\uszips.csv', index = None, header=True)

zip_df = pd.read_csv(r'D:\data\Water Fluoridation\zip\uszips.csv', dtype={'zip': str})

zip_df['zip']=zip_df['zip'].str.rjust(5, "0")


pp = pprint.PrettyPrinter(indent=4)
print('Postal codes dataframe shape: ', zip_df.shape)
print('Missing information in % rounded to 2 decimals')
pp.pprint((zip_df.isna().sum() / zip_df.shape[0]).round(2))

# variables not needed
# density county_weights imprecise county_names_all county_fips_all millitary 
# drop these variables

droped = ['county_weights', 'imprecise', 'military','county_names_all', 'county_fips_all']
zip_df.drop(droped, axis=1, inplace=True)

zip_df['state_name'] = zip_df['state_name'].fillna(zip_df['state_id'].map(missing_state))


# use zip_df.isna().sum() to check missing data. We find that no zip code
# is missing from the data.

# scrapping
# ewg url: https://www.ewg.org/tapwater/search-results.php?zip5=ZIP_CODE&searchtype=zip
# consider only the relevant parts of US
relevant_states = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 
    'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 
    'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 
    'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 
    'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 
    'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 
    'West Virginia', 'Wisconsin', 'Wyoming']


zip_df = zip_df[zip_df['state_name'].isin(relevant_states)]

#

BASE_URL = 'https://www.ewg.org/tapwater/'
SEARCH_URL_START = 'search-results.php?zip5='
SEARCH_URL_END = '&searchtype=zip'

url = 'https://www.ewg.org/tapwater/search-results.php?zip5=96799&searchtype=zip'
r = requests.get(url, headers = header)
soup = BeautifulSoup(r.content, 'html.parser')

def got_results_from_url(soup, url):
    error = soup.find('h2', text = 'No systems found that match your search')
    if (error):
        return False
    else:
        return True
    
got_results_from_url(soup, url)

def get_info(s):
    start = s.find('<p>') + 3
    end = s.find('</p>', start)
    return s[start: end]




def get_pws(s):
    start = s.find('pws=') + 4
    end = s.find('"', start)
    return s[start: end]





def generate_url_from_zip(zip_value):
    return BASE_URL + SEARCH_URL_START + zip_value + SEARCH_URL_END

def get_population(people_served_tag):
    return int(people_served_tag.replace('Population served:', '').replace(',',''))

def get_city(element):
    return element.text.split(',')[0].strip()

def extract_info_from_row(elements):
    row_info = {}
    row_info['url'] = BASE_URL + elements[0].find('a')['href']
    row_info['utility_name'] = elements[0].text
    row_info['city'] = get_city(elements[1])
    row_info['people_served'] = get_population(elements[2].text)
    return row_info

def process_results(results, zip_value, state_id, zcta, county_fips, female, income_household_median, home_ownership, education_college_or_above, race_white):
    zip_results = []
    result_rows = results.find_all('tr')
    for row in result_rows:
        elements = row.find_all('td')
        if elements:
            element = extract_info_from_row(elements)
            element['zip'] = zip_value
            element['state'] = state_id
            element['zcta'] = zcta
            element['county_fips'] = county_fips
            element['female'] = female
            element['median_income'] = income_household_median
            element['educ'] = education_college_or_above
            element['home'] = home_ownership
            element['race'] = race_white
            zip_results.append(element)
    return zip_results
            
def no_table(results, zip_value, state_id, zcta, county_fips, female, income_household_median, home_ownership, education_college_or_above, race_white):
    zip_results = []
    pws = str(results.find_all('a'))
    info = str(results.find_all('p')).split(",", 2)
    row_info = {}
    row_info['url'] = get_pws(pws)
    row_info['utility_name'] = get_info(info[0])
    row_info['city'] = get_info(info[1])
    row_info['people_served'] = get_info(info[2])
    row_info['zip'] = zip_value
    row_info['state'] = state_id
    row_info['zcta'] = zcta
    row_info['county_fips'] = county_fips
    row_info['female'] = female
    row_info['median_income'] = income_household_median
    row_info['educ'] = education_college_or_above
    row_info['home'] = home_ownership
    row_info['race'] = race_white
    zip_results.append(row_info)
    return zip_results
    

def process_zip(zip_value, state_id, zcta, county_fips, female, income_household_median, home_ownership, education_college_or_above, race_white):
    url = generate_url_from_zip(zip_value)
    r = s.get(url, headers = header)
    soup = BeautifulSoup(r.content, 'html.parser')
    if got_results_from_url(soup, url):
        results = soup.find_all('table', {'class': 'search-results-table'})
        if results != []:
            return process_results(results[0], zip_value, state_id, zcta, county_fips, female, income_household_median, home_ownership, education_college_or_above, race_white)
        else:
            results = soup.find_all('div', {'class': 'featured-utility'})
            return no_table(results[0], zip_value, state_id, zcta, county_fips, female, income_household_median, home_ownership, education_college_or_above, race_white)
    else:
        return []
    
# if results == []:
#           results = soup.find_all('div', {'class': 'featured-utility'})

    



def scrap_ewg_tap_water_database(df):
    data = []
    
    status = 0
    bar = progressbar.ProgressBar(max_value=df.shape[0])
    
    for index, row in df.iterrows():
        bar.update(status)        
        status = status + 1
        utilities = process_zip(row['zip'], row['state_id'], row['zcta'], row['county_fips'], row['female'], row['income_household_median'], row['home_ownership'], row['education_college_or_above'], row['race_white'])
        data = data + utilities
        bar.finish() 
    return data



ewg_tap_water = scrap_ewg_tap_water_database(zip_df)
ewg_tap_water_df = pd.DataFrame(ewg_tap_water)
ewg_tap_water_df['url'] = ewg_tap_water_df['url'].str[-9:]
ewg_tap_water_df = ewg_tap_water_df.rename(columns = {'url' : 'pws'} )
ewg_tap_water_df = ewg_tap_water_df[ewg_tap_water_df['pws'] != '.php?pws=']
ewg_tap_water_df.to_csv('ewg.csv')


# 

os.chdir("D:\data\Water Fluoridation\MWF")
path = os.getcwd()
files = os.listdir(path)

wfr_16_list = [f for f in files if f[3:9] == "16.csv"]
wfr_17_list = [f for f in files if f[3:9] == "17.csv"]
wfr_18_list = [f for f in files if f[3:9] == "18.csv"]
wfr_19_list = [f for f in files if f[3:9] == "19.csv"]


wfr_16 = []

for f in wfr_16_list:
    df = pd.read_csv(f, encoding = 'unicode_escape',usecols=['PWS ID', 'Population Served', 'Fluoridated','Fluoride Conc.'])
    df = df.rename(columns = {'PWS ID':'pws'})
    df['pws'] = df['pws'].replace('-', '', regex=True)
    df['pws'] = df['pws'].replace(' ', '')
    
    df['year'] = '2016'
    wfr_16.append(df)
    

    
wfr_17 = []

for f in wfr_17_list:
    df = pd.read_csv(f, encoding = 'unicode_escape',usecols=['PWS ID', 'Population Served', 'Fluoridated','Fluoride Conc.'])
    df = df.rename(columns = {'PWS ID':'pws'})
    df['pws'] = df['pws'].replace('-', '', regex=True)
    df['pws'] = df['pws'].replace(' ', '')
    df['year'] = '2017'
    wfr_17.append(df)
    
    
wfr_18 = []

for f in wfr_18_list:
    df = pd.read_csv(f, encoding = 'unicode_escape',usecols=['PWS ID', 'Population Served', 'Fluoridated','Fluoride Conc.'])
    df = df.rename(columns = {'PWS ID':'pws'})
    df['pws'] = df['pws'].replace('-', '', regex=True)
    df['pws'] = df['pws'].replace(' ', '')
    df['year'] = '2018'
    wfr_18.append(df)
    

wfr_19 = []

for f in wfr_19_list:
    df = pd.read_csv(f, encoding = 'unicode_escape',usecols=['PWS ID', 'Population Served', 'Fluoridated','Fluoride Conc.'])
    df = df.rename(columns = {'PWS ID':'pws'})
    df['pws'] = df['pws'].replace('-', '', regex=True)
    df['pws'] = df['pws'].replace(' ', '')
    df['year'] = '2019'
    wfr_19.append(df)
    
ewg_df = pd.read_csv(r'D:\data\Water Fluoridation\zip\ewg.csv', dtype={'people_served': str})
    

wfr_16_df = pd.concat(wfr_16)
wfr_17_df = pd.concat(wfr_17)
wfr_18_df = pd.concat(wfr_18)
wfr_19_df = pd.concat(wfr_19)   

wfr_16_df = wfr_16_df.drop_duplicates(subset = ['pws'])
wfr_17_df = wfr_17_df.drop_duplicates(subset = ['pws'])
wfr_18_df = wfr_18_df.drop_duplicates(subset = ['pws'])
wfr_19_df = wfr_19_df.drop_duplicates(subset = ['pws'])


wfr_16_df.reset_index(drop = True)
wfr_17_df.reset_index(drop = True) 
wfr_18_df.reset_index(drop = True) 
wfr_19_df.reset_index(drop = True)  

wfr_16_df.to_csv('wfr_16_con.csv')
wfr_17_df.to_csv('wfr_17_con.csv')
wfr_18_df.to_csv('wfr_18_con.csv')
wfr_19_df.to_csv('wfr_19_con.csv')


wfr_4y = [wfr_16_df,wfr_17_df,wfr_18_df,wfr_19_df]
wfr_4y_df = pd.concat(wfr_4y)

wfr_4y_df.reset_index()

wfr_4y_df.to_csv('wfr_4y.csv')


#county map
df_16 = pd.read_csv('16.csv', dtype={'county_fips': str})
df_16['county_fips']=df_16['county_fips'].str.rjust(5, "0")
df_16 = df_16.rename(columns = {'pct_2':'% served'})
df_17 = pd.read_csv('17.csv', encoding = 'unicode_escape', dtype={'county_fips': str})
df_17['county_fips']=df_17['county_fips'].str.rjust(5, "0")
df_17 = df_17.rename(columns = {'pct_2':'% served'})
df_18 = pd.read_csv('18.csv', encoding = 'unicode_escape', dtype={'county_fips': str})
df_18['county_fips']=df_18['county_fips'].str.rjust(5, "0")
df_18 = df_18.rename(columns = {'pct_2':'% served'})
df_19 = pd.read_csv('19.csv', encoding = 'unicode_escape', dtype={'county_fips': str})
df_19['county_fips']=df_19['county_fips'].str.rjust(5, "0")
df_19 = df_19.rename(columns = {'pct_2':'% served'})


# Read in unemployment rates
df_16.to_csv('161.csv')
unemployment = {}
min_value = 100; max_value = 0
reader = csv.reader(open('161.csv'), delimiter=",")
for row in reader:
    try:
        full_fips = row[1]
        rate = float( row[3].strip() )
        unemployment[full_fips] = rate
    except:
        pass
    

 
#If I remember correctly, the following cannot procceed 
# Load the SVG map
svg = open('counties.svg', 'r').read()
 
# Load into Beautiful Soup
soup = BeautifulSoup(svg, selfClosingTags=['defs','sodipodi:namedview'])
 
# Find counties
paths = soup.findAll('path')
 
# Map colors
colors = ["#F1EEF6", "#D4B9DA", "#C994C7", "#DF65B0", "#DD1C77", "#980043"]
 
# County style
path_style = 'font-size:12px;fill-rule:nonzero;stroke:#FFFFFF;stroke-opacity:1;stroke-width:0.1;stroke-miterlimit:4;stroke-dasharray:none;stroke-linecap:butt;marker-start:none;stroke-linejoin:bevel;fill:'
 
# Color the counties based on unemployment rate
for p in paths:
     
    if p['id'] not in ["State_Lines", "separator"]:
        try:
            rate = unemployment[p['id']]
        except:
            continue
             
         
        if rate > 10:
            color_class = 5
        elif rate > 8:
            color_class = 4
        elif rate > 6:
            color_class = 3
        elif rate > 4:
            color_class = 2
        elif rate > 2:
            color_class = 1
        else:
            color_class = 0
 
 
        color = colors[color_class]
        p['style'] = path_style + color
 
print (soup.prettify())

sss = soup.prettify()





  







