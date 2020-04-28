import requests
import json
import configparser
import sys
import os
from datetime import datetime
from opencage.geocoder import OpenCageGeocode
from pprint import pprint



# config['DEFAULT'] = {'ServerAliveInterval': '45','Compression': 'yes','CompressionLevel': '9'}
# with open('../../test.cfg', 'w') as configfile:
# 	config.write(configfile)

def station_information_raw():
	url = config['SITE']['STATION_INFORMATION']
	data = requests.get(url).json()

	with open('../../dataset/station_information.json', 'w') as f:
		json.dump(data, f)


def station_status_raw():
	epoch = []


	url = config['SITE']['STATION_STATUS']
	data = requests.get(url).json()
	last_updated = data['last_updated']

	if last_updated in epoch:
		pass
	else:
		with open('../../dataset/station_status/'+ str(last_updated) + '.txt', 'w') as f:
			json.dump(data, f)
			epoch.append(last_updated)

def free_bike_raw():
	epoch = []


	url = config['SITE']['FREE_BIKE_STATUS']
	data = requests.get(url).json()
	last_updated = data['last_updated']

	if last_updated in epoch:
		pass
	else:
		with open('../../dataset/station_status/'+ str(last_updated) + '.txt', 'w') as f:
			json.dump(data, f)
			epoch.append(last_updated)


def main():
	# while True:
	# 	station_status_raw()
	#free_bike_raw()

	config = configparser.ConfigParser(allow_no_value=True)
	config.read('../../config.cfg')

	key = config['ADDR']['KEY']

	geocoder = OpenCageGeocode(key)

	results = geocoder.reverse_geocode(37.812314, -122.260779)

	print(results)
	

if __name__ =="__main__":
	main()



