#!flask/bin/python
from flask import Flask, jsonify, request, has_app_context, current_app, g
import requests
import datetime
from DatabaseManager import DatabaseManager
from BookerAPI import BookerAPIClient

def percentagebar(small, large, size):
	s, l, barsize = float(small), float(large), float(size)
	bars = int(round((s/l) * barsize))
	notbars = int(barsize - bars)
	return "[" + "="*bars + " "*notbars + "]"

class DashboardApp:
	instance = None
	app = Flask(__name__)
	BookerClient = None
	DBManager = None
	
	def __init__(self):
		self.BookerClient = BookerAPIClient()
		self.DBManager = DatabaseManager("sql", True, user = "ella-bliss", password = "T3nuispinus!", host = "localhost", database = "ELLABLISS")
	
	def run(self, debug=False):
		self.app.run(debug=debug, host='ella-bliss.lab.diodon.xyz')
	
	@app.route('/')
	def index():
		return "Hello, World!"
	
	@app.route('/SearchNewOrders', methods=['GET'])
	def populate(): #this will be alongside an "update" endpoint that works the same way, but with the query (most recent update to local db) -> (now)
		locationID = request.args.get('locationID')
		pagesize = 20 #this could be anything < 1 probably. haven't tested much
		DashboardApp.instance.DBManager.Cursor.execute("SELECT `DateCreated` FROM `Order` ORDER BY DateCreated DESC LIMIT 1")
		(startDate,) = DashboardApp.instance.DBManager.Cursor.fetchone()
		#~ startDate = startDate.replace(day=1,hour=0,minute=0,second=0) #this would start at the beginning of most recently updated month. shouldn't be necessary bc FindOrders sorts on DateCreated
		return "Starting on %s" % (str(startDate))
		
		fromDate = startDate
		#nextFirst is next 1st day of the month, where next loop will start
		#used to calculate toDate (one day before, due to 
		nextFirst = fromDate.replace(day=1,hour=0,minute=0,second=0)+relativedelta(months=+1)
		
		API = DashboardApp.instance.BookerClient
		
		#up to and including this month
		while fromDate < datetime.datetime.now():
			#fromDate -> 1st day of that month -> first day of next month -> last day of this month
			toDate = nextFirst+relativedelta(days=-1)
			
			timer = API.stats["Time Cumulative"]
			resultcount = API.FindOrders(locationID, fromDate, toDate, 0, 0)["TotalResultsCount"]
			print("%s: %s matching orders (%.2f seconds)" % (fromDate.strftime("%B, %Y"), resultcount, API.stats["Time Cumulative"]-timer))
			numpages = int(math.ceil(float(resultcount)/pagesize))
			
			timer_start = time.time()
			for pagenumber in range(1, numpages + 1):
				print("Getting page %d/%d..." % (pagenumber, numpages))
				timer = time.time()
				data = API.FindOrders(locationID, fromDate, toDate, pagenumber, pagesize)
				if data is None:
					print("refreshing location token again")
					API.get_location_token(locationID, True)
					data = API.FindOrders(locationID, fromDate, toDate, pagenumber, pagesize)
				dbm.StoreOrders(data["Results"])
				
				#time estimation
				timer_end = time.time()
				#estimated_time = elapsed / (1 - (float(pagenumber)/numpages)) <- I think?
				#so estimated_time = (timer_end - timer_start) * (1 - (float(numpages)/pagenumber))
				elapsed = timer_end - timer_start
				total_time = elapsed  / (float(pagenumber) / numpages)
				estimated_time = total_time - elapsed
				print("Last request: %.2fs\t Estimated time: %s" % (timer_end - timer, str(datetime.timedelta(seconds=round(estimated_time)))))
				#~ print(percentagebar(pagenumber, numpages, 30))
			
			#increment month
			fromDate = nextFirst
			nextFirst = fromDate+relativedelta(months=+1)
		
		dbm.close()

	def DateRangeSummary(fromDate, toDate):
		return self.DBManager.DateRangeSummary(fromDate, toDate)

if __name__ == "__main__":
	DashboardApp.instance = DashboardApp()
	DashboardApp.instance.run(True)

