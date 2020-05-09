# This script retrieves data from 6 weather stations:
# ['74506', 'KSJC0', '72494', '72493', 'KRHV0']
# and write the weather data to weather_data.csv
import requests
import json
import configparser
import sys
import os
from datetime import datetime
from pprint import pprint
import pandas as pd
import random
from time import sleep

def get_weather_data_daily(station, start_date, end_date):
	config = configparser.ConfigParser(allow_no_value=True)
	config.read('../../config.cfg')
	key = config['ADDR']['KEY']

	link = 'https://api.meteostat.net/v1/history/daily?station=' + \
			station + \
			'&start=' + start_date + \
			'&end=' + end_date + \
			'&key=' + key

	data = requests.get(link).json()

	return data


def main():
	stations = ['74506', 'KSJC0', '72494', '72493', 'KRHV0']
	start_date = '2019-01-01'
	end_date = '2020-03-31'

	weather_data = (get_weather_data_daily(station, start_date, end_date) for station in stations)

	print(next(weather_data)['data'])
	data_KSJC0 = pd.DataFrame(next(weather_data)['data'])
	data_KSJC0['station'] = 'KSJC0'
	data_72494 = pd.DataFrame(next(weather_data)['data'])
	data_72494['station'] = '72494'
	data_72493 = pd.DataFrame(next(weather_data)['data'])
	data_72493['station'] = '72493'


	data = pd.concat([data_KSJC0, data_72494, data_72493])

	data.to_csv("../../dataset/weather_data/weather_data.csv", index=False)
	

if __name__ =="__main__":
	main()