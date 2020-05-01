# This script extract coordinates from station_information.csv,
# constructs API links with the coordinates,
# get weather station ID with the API links,
# and write station_information_with_wsid.csv.
import requests
import json
import configparser
import sys
import os
from datetime import datetime
from opencage.geocoder import OpenCageGeocode
from pprint import pprint
import pandas as pd
import random
from time import sleep

def get_coordinates():
	"""
	Extract coordinate from station_data.csv
	"""
	station = pd.read_csv("../../dataset/station_data/station_data.csv")

	coordinates = station[['lon', 'lat']].to_records(index=False)

	return coordinates


def weather_station(links):
	"""
	- Input a list of links and ouput weather station information
	- API: https://api.meteostat.net/#history
	"""
	for link in links:
		try:
			data = requests.get(link).json()
			yield data['data'][0]['id']
		except json.decoder.JSONDecodeError:
			yield 'na'


def main():
	config = configparser.ConfigParser(allow_no_value=True)

	config.read('../../config.cfg')

	key = config['ADDR']['KEY']

	coordinates = get_coordinates()

	links = ['https://api.meteostat.net/v1/stations/nearby?lat=' + str(i[1]) + '&lon=' + str(i[0]) + '&limit=5&key=' + key for i in coordinates]

	weather_stations = list(weather_station(links))

	station = pd.read_csv("../../dataset/station_data/station_data.csv")

	station['weather_station_id'] = weather_stations

	station.to_csv("../../dataset/station_data/station_data1.csv")
	
if __name__ =="__main__":
	main()



