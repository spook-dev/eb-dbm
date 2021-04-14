#!/usr/bin/python3
import requests
import json
from time import time
import pytz
import datetime

class BookerAPIClient:
	credentials = {"location_token":{}}
	locations = []
	brandID = None
	stats = {
		"Time Cumulative": 0,
		"Total Requests": 0
	}
	
	def __init__(self):
		#load in credentials from text file (or DB in future?)
		self.get_credentials()
	
	#return missing credentials from -
	parameter_names = ["client_id", "client_secret", "Ocp-Apim-Subscription-Key", "personal_access_token"]
	def missing_parameters(self):
		missing_params = []
		for parameter_name in self.parameter_names:
			if parameter_name not in self.credentials:
				missing_params.append(parameter_name)
		return missing_params
	
	#load self.credentials from file (client ID/secret, subscription key, personal access token)
	def get_credentials(self, force_reload = False):
		#tk move credentials to DB?
		if len(self.missing_parameters()) == 0 and not force_reload:
			return self.credentials
		
		with open(".mindbody-credentials.txt", "r") as f:
			for line in f.readlines():
				linedata = line.replace("\r", "").replace("\n", "").split("=")
				if linedata[0] in self.parameter_names:
					self.credentials[linedata[0]] = "=".join(linedata[1:])
			missing_params = self.missing_parameters()
			if len(missing_params) > 0:
				print(".mindbody-credentials.txt must include the following keys: " + " ".join(missing_params))
				exit(1)
		return self.credentials
	
	def get_json(self, url, *args, **kwargs):
		try:
			timer = time()
			response = requests.get(url, *args, **kwargs)
			timer = time() - timer
			data = response.json()
			self.stats["Time Cumulative"] += timer
			self.stats["Total Requests"] += 1
			return data
		except Exception as e:
			print(response.content)
	
	def post_json(self, url, *args, **kwargs):
		try:
			timer = time()
			response = requests.post(url, *args, **kwargs)
			timer = time() - timer
			data = response.json()
			self.stats["Time Cumulative"] += timer
			self.stats["Total Requests"] += 1
			return data
		except Exception as e:
			print(response.content)
	
	#load self.brands from https://api-staging.booker.com/v4.1/merchant/location/brands
	def get_brandID(self, force_reload = False):
		if self.brandID is not None and not force_reload:
			return self.brandID
		response = self.get_json(
			"https://api-staging.booker.com/v4.1/merchant/location/brands",
			params = {
				"access_token": self.get_brand_token(),
				"accountName": "ellablissparent"
			},
			headers = {
				"Ocp-Apim-Subscription-Key": self.get_credentials()["Ocp-Apim-Subscription-Key"]
			}
		)
		
		self.brandID = response["LookupOptions"][0]["ID"]
		return self.brandID
	
	#load self.credentials["brand_access_token"] from https://api-staging.booker.com/v5/auth/connect/token
	def get_brand_token(self, force_reload = False):
		if "brand_access_token" in self.credentials.keys() and not force_reload:
			return self.credentials["brand_access_token"]
		
		response = self.post_json(
			"https://api-staging.booker.com/v5/auth/connect/token",
			data = {
				"grant_type": "personal_access_token",
				"client_id": self.get_credentials()["client_id"],
				"client_secret": self.get_credentials()["client_secret"],
				"scope": "merchant",
				"personal_access_token": self.get_credentials()["personal_access_token"]
			},
			headers = {
				"Content-Type": "application/x-www-form-urlencoded",
				"Ocp-Apim-Subscription-Key": self.get_credentials()["Ocp-Apim-Subscription-Key"]
			}
		)
		
		self.credentials["brand_access_token"] = response["access_token"]
		return self.credentials["brand_access_token"]
	
	#load self.locations from https://api-staging.booker.com/v4.1/merchant/locations
	def get_locations(self, force_reload = False):
		if len(self.locations) > 0 and not force_reload:
			return self.locations
		
		response = self.post_json(
			"https://api-staging.booker.com/v4.1/merchant/locations",
			json = {
				"UsePaging": "true",
				"PageNumber": 1,
				"PageSize": 20,
				"SortBy": [
					{
						"SortBy": "Name",
						"SortDirection": 0
					}
				],
				"access_token": self.get_brand_token()
			},
			headers = {
				"Content-Type": "application/json",
				"Ocp-Apim-Subscription-Key": self.get_credentials()["Ocp-Apim-Subscription-Key"]
			}
		)
		
		self.locations = response["Results"]
		return self.locations
	
	#create a location token from https://api-staging.booker.com/v5/auth/context/update
	def get_location_token(self, locationID, force_reload = False):
		if locationID in self.credentials["location_token"].keys() and not force_reload:
			return self.credentials["location_token"][locationID]
		
		response = self.post_json(
			"https://api-staging.booker.com/v5/auth/context/update",
			params = {
				"locationId": locationID,
				"brandId": self.get_brandID()
			},
			headers = {
				"Ocp-Apim-Subscription-Key": self.get_credentials()["Ocp-Apim-Subscription-Key"],
				"Authorization": "Bearer " + self.get_brand_token()
			}
		)
		
		self.credentials["location_token"][locationID] = response
		return self.credentials["location_token"][locationID]
	
	@staticmethod
	def FormatDateTZ(date, tz):
		#~ fmt = '%Y-%m-%dT%H:%M:%S-0700' #non-daylight savings aware
		fmt = '%Y-%m-%dT%H:%M:%S%z'
		return date.astimezone(tz).strftime(fmt)
	
	def FormatDate(self, date):
		return BookerAPIClient.FormatDateTZ(date, pytz.timezone("US/Mountain")) #tk fetch timezone from location info
	
	def FindOrders(self, locationID, fromDate, toDate, page, pagesize):
		response = self.post_json(
			"https://api-staging.booker.com/v4.1/merchant/orders/partial",
			json = {
				"LocationID": locationID,
				"UsePaging": True,
				"PageNumber": page,
				"PageSize": pagesize,
				"SortBy": [
					{
						"SortBy": "DateCreated",
						"SortDirection": 0
					}
				],
				"FromDateCreatedOffset": self.FormatDate(fromDate),
				"ToDateCreatedOffset": self.FormatDate(toDate),
				"access_token": self.get_location_token(locationID)
			},
			headers = {
				"Ocp-Apim-Subscription-Key": self.get_credentials()["Ocp-Apim-Subscription-Key"],
				"Content-Type": "application/json"
			}
		)
		return response

#testing
if __name__ == "__main__":
	API = BookerAPIClient()
	#~ print(API.get_location_token(30234))
	#~ exit()
	
	fromDate = datetime.datetime(2000, 1, 1, 0, 0, 0)
	toDate = datetime.datetime(2021, 12, 31, 23, 59, 59)
	print(API.FormatDate(fromDate), API.FormatDate(toDate))
	#~ exit()
	print(API.FindOrders(30234, fromDate, toDate, 1, 1))
	#~ print(API.FindOrders(30234, fromDate, toDate, 1, 1)["TotalResultsCount"], " Results found")
	print(API.stats)
	#~ results = API.FindOrders(30234, fromDate, toDate, 1, 1)
	#~ print(results["Results"][0]["DateCreatedOffset"])
	#~ print(API.stats)
