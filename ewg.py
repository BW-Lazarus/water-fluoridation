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


# scrapping
relevant_states = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 
    'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 
    'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 
    'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 
    'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 
    'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 
    'West Virginia', 'Wisconsin', 'Wyoming']


zip_df = zip_df[zip_df['state_name'].isin(relevant_states)]


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







  







