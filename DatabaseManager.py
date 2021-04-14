#!/usr/bin/python3

import mysql.connector
from mysql.connector import errorcode

import json
from pathlib import Path
from math import ceil
from time import time
from datetime import datetime, date, timedelta
from dateutil.relativedelta import *

class Field:
	mySQL_signature = None
	name = None
	sqlauto = False
	def __init__(self, name, mySQL_signature, sqlauto = False):
		self.name = name
		self.mySQL_signature = mySQL_signature
		self.sqlauto = sqlauto
	def csv(self):
		return self.name
	def sql(self):
		return "`%s` %s" % (self.name, self.mySQL_signature)
#~ class ForeignKey(Field):
	#~ refTable = None
	#~ refField = None
	#~ def __init__(self, name, mySQL_signature, refTable, refField):
		#~ Field.__init__(self, name, mySQL_signature)
		#~ self.refTable = refTable
		#~ self.refField = refField
	#~ def sql(self):
		#~ return "FOREIGN KEY `%s` REFERENCES `%s` `%s` ON DELETE CASCADE" % (self.name, self.refTable, self.refField)

class Table:
	name = None
	fields = None
	primary_key = None
	def __init__(self, name, fields, primary_key = None):
		self.name = name
		self.fields = fields
		if primary_key is None:
			#create autogen primary key field of the form nameID or nameID0,nameID1,...
			primary_key = name + "ID"
			c = -1
			while primary_key in [field.name for field in fields]:
				c += 1
				primary_key = name + "ID" + str(c)
			fields.append(Field(primary_key, "INT NOT NULL AUTO_INCREMENT", True))
		self.primary_key = primary_key
	def sql_create(self):
		return "CREATE TABLE `%s` (\n%s,\nPRIMARY KEY(`%s`)\n);" % (self.name, ",\n".join([field.sql() for field in self.fields]), self.primary_key)
	def sql_fields(self):
		return [field.name for field in self.fields]
	def csv_filename(self):
		return self.name + ".csv"
	def csv_fields(self):
		return [field.name for field in self.fields if not field.sqlauto]
	def csv_header(self):
		return CSV_DELIMITER.join(self.csv_fields())
class TypeTable(Table):
	def __init__(self, name):
		Table.__init__(self, 
			name, 
			[
				Field("ID", "INT NOT NULL"),
				Field("Name", "VARCHAR(255) NOT NULL")
			],
			"ID"
		)

CSV_DELIMITER = "\t"
class CSVManager:
	csvPath = None
	
	def __init__(self):
		#check args, initialize database path in filesystem & csvPath variable
		required_args = ['path']
		argdiff = set(required_args) - set(kwargs.keys())
		if len(argdiff) > 0:
			print("Please construct %s with the following kwargs to initialize in sql mode: " % (type(self).__name__, ", ".join(required_args)))
			exit(1)
		self.csvPath = Path(kwargs['path'])
		self.csvPath.mkdir(parents = True, exist_ok = True)
	
	def init_tables(self):
		#for each table, create (if needed) csv with proper headers
		for table in self.Tables.values():
			path = self.csvPath / table.csv_filename()
			if not path.exists() or not self.append:
				path.write_text(table.csv_header())
	
	def Store(self, tableName, tableObject):
		table = self.Tables[tableName]
		#for TypeTables we can read through and make sure we don't duplicate IDs
		if isinstance(table, TypeTable):
			typeTable = {tableObject["ID"]: tableObject["Name"]}
			tableCSV = self.csvPath / table.csv_filename()
			with tableCSV.open() as f:
				lines = f.readlines()
				for line in lines[1:]: #skip the header line
					linedata = line.replace("\r", "").replace("\n", "").split("\t")
					key, value = linedata[0], "\t".join(linedata[1:])
					if len(key) > 0 and int(key) not in typeTable.keys():
						typeTable[int(key)] = value
			tableCSV.write_text("\n".join([lines[0].replace("\r", "").replace("\n", "")] + [
				"\t".join([str(key), value]) for (key, value) in typeTable.items()
			]))
		#for regular tables, just append
		else:
			tableCSV = self.csvPath / table.csv_filename()
			with tableCSV.open("a") as f:
				f.write("\n" + "\t".join([str(tableObject[field]) for field in table.csv_fields()]))
	
	def format_date(self, apidate):
		if apidate is not None:
			return apidate[:10]
	
	def StoreOrders(self, orders):
		for order in orders:
			self.Store("OrderStatus", order["Status"]) #TypeTables are handled specially so this works with no modification
			if order["Status"]["ID"] == 6:
				continue #tk currently no handling in excel for void orders
			
			#Populate order fields from API jsons
			Order = {
				"OrderID": order["ID"],
				"LocationID": order["LocationID"],
				"OrderNumber": order["OrderNumber"],
				"StatusID": order["Status"]["ID"],
				"DatePaid": self.format_date(order["DatePaidOffset"]),
				"DateCreated": self.format_date(order["DateCreatedOffset"]),
				"PrePaid": 0,
				"Tax": order["TotalTaxesRounded"]["Amount"]
			}
			#add together gift certificate payments to get prepaid amount
			for payment in order["Payment"]["PaymentItems"]:
				if payment["Method"]["Name"] == "Gift Certificate":
					Order["PrePaid"] -= payment["Amount"]["Amount"]
			#fix floating point rounding errors from api. Round USD to 4 decimals for convention/match SQL
			Order["PrePaid"] = round(Order["PrePaid"], 4)
			
			#items and refunds, recursively, follow the same structure
			for item in order["Items"]:
				self.Store("ItemType", item["Type"])
				if not item["Type"]["Name"] in ["Treatment", "ProductVariant", "GiftCertificate", "Employee Tip", "Appointment Cancellation", "Treatment Add-On", "Membership Initiation", "Membership Fee", "GiftCardRefill", "Package Add-On", "Package"]:
					print(json.dumps(item))
				
				Item = {
					"OrderID": item["OrderID"],
					"OrderItemID": item["ID"],
					"Type": item["Type"]["ID"],
					"IsService": item["IsService"],
					"EmployeeName": item["EmployeeName"],
					"DisplayName": item["DisplayName"],
					"FinalPrice":item["DynamicPrice"]["FinalPrice"]["Amount"],
					"OriginalTagPrice":item["DynamicPrice"]["OriginalTagPrice"]["Amount"],
					"Quantity": item["Quantity"]
				}
				
				for refund in item["Refunds"]:
					self.Store("RefundType", refund["Type"])
					self.Store("RefundPaymentMethod", refund["PaymentMethod"])
					
					Refund = {
						"OrderID": refund["OrderID"],
						"OrderItemID": refund["OrderItemID"],
						"Amount": refund["Amount"]["Amount"],
						"PaymentMethod": refund["PaymentMethod"]["ID"],
						"Type": refund["Type"]["ID"],
						"DateCreated": self.format_date(refund["DateCreatedOffset"]),
						"TotalTax": refund["TotalTax"]["Amount"]
					}
					
					self.Store("Refund", Refund)
				self.Store("Item", Item)
			self.Store("Order", Order)

class DatabaseManager:
	#This is the schema being used. Changes here will filter through everything assuming you do a non-append run first
	#verify in StoreOrders, when creating records to be stored, they share names with the schema fields.
	#records do not have to have all fields in table, but each field in the record must exist in the table
	Tables = {
		"Location": Table("Location",
			[
				Field("LocationID", "INT NOT NULL"),
				Field("BusinessName", "VARCHAR(255) NOT NULL") #tk is it actually called DisplayName?
			],
			"LocationID"
		),
		"OrderStatus": TypeTable("OrderStatus"),
		"Order": Table("Order",
			[
				Field("OrderID", "INT NOT NULL"),
				Field("LocationID", "INT NOT NULL REFERENCES Location.LocationID"),
				Field("OrderNumber", "CHAR(12) NOT NULL"),
				Field("StatusID",  "INT NOT NULL REFERENCES OrderStatus.ID"),
				Field("DatePaid", "DATETIME"),
				Field("DateCreated", "DATETIME"),
				Field("PrePaid", "decimal(19,4)"),
				Field("Tax", "decimal(19,4)"),
				Field("updated", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", True)
			],
			"OrderID"
		),
		"ItemType": TypeTable("ItemType"),
		"Item": Table("Item",
			[
				Field("OrderID", "INT NOT NULL REFERENCES Order.OrderID"),
				Field("OrderItemID", "INT NOT NULL"),
				Field("Type", "INT NOT NULL REFERENCES ItemType.ID"),
				Field("IsService", "BOOL NOT NULL"),
				Field("EmployeeName", "VARCHAR(255)"),
				Field("DisplayName", "VARCHAR(255)"),
				Field("FinalPrice", "decimal(19,4)"),
				Field("OriginalTagPrice", "decimal(19,4)"),
				Field("Quantity", "INT")
			],
			"OrderItemID"
		),
		"RefundType": TypeTable("RefundType"),
		"RefundPaymentMethod": TypeTable("RefundPaymentMethod"),
		"Refund": Table("Refund",
			[
				Field("OrderID", "INT NOT NULL REFERENCES Order.OrderID"),
				Field("OrderItemID", "INT NOT NULL REFERENCES Item.OrderItemID"),
				Field("Amount", "decimal(19,4)"),
				Field("PaymentMethod", "INT NOT NULL REFERENCES RefundPaymentMethod.ID"),
				Field("Type", "INT NOT NULL REFERENCES RefundType.ID"),
				Field("DateCreated", "DATETIME"),
				Field("TotalTax", "decimal(19,4)")
			]
		)
	}
	
	Config = {}
	Connection = None
	Cursor = None
	
	#initialize above variables, and init tables as CSVs or SQL tables
	def __init__(self, **kwargs):
		#check args, connect to database. Initialize Config, Connection, Cursor
		required_args = ['user', 'password', 'host', 'database']
		argdiff = set(required_args) - set(kwargs.keys())
		if len(argdiff) > 0:
			print("Please construct %s with the following kwargs to initialize in sql mode: " % (type(self).__name__, ", ".join(required_args)))
			exit(1)
		self.Config = {
			'user': kwargs['user'],
			'password': kwargs['password'],
			'host': kwargs['host'],
			'database': kwargs['database'],
			'raise_on_warnings': True
		}
		try:
			self.Connection = mysql.connector.connect(**self.Config)
			self.Connection.time_zone = '-07:00' #tk should this be modular?
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print("SQL authentication failure")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("Database does not exist")
			else:
				print(err)
			exit(1)
		self.Cursor = self.Connection.cursor()
		self.init_tables()
	
	def nuke_db(self):
		try:
			self.Cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
			self.Cursor.execute("DROP TABLE IF EXISTS `" + "`,`".join(self.Tables.keys()) + "`;")
			self.Cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
			self.Connection.commit()
		except mysql.connector.Error as err:
			print("error while clearing tables:")
			print(err.msg)
			if input("continue? (Y/n)").lower() != "y": #this probably isn't a critical error
				self.close()
				exit(1)
	
	def init_tables(self):
		#verify all tables exist. if it does exist (errorcode.ER_TABLE_EXISTS_ERROR) the error is ignored
		for table in self.Tables.values():
			try:
				self.Cursor.execute(table.sql_create())
				self.Connection.commit()
			except mysql.connector.Error as err:
				if err.errno != errorcode.ER_TABLE_EXISTS_ERROR:
					print("error while creating table %s:" % (table.name))
					print(err.msg)
					if input("continue? (Y/n)").lower() != "y": #this probably isn't a critical error
						self.close()
						exit(1)
	
	def close(self):
		self.Connection.commit()
		self.Cursor.close()
		self.Connection.close()
	
	def Store(self, tableName, tableObject):
		table = self.Tables[tableName]
		try:
			add_record = "INSERT INTO `%s` (`%s`) VALUES (%%(%s)s)" % (tableName, "`, `".join(tableObject.keys()), ")s, %(".join(tableObject.keys()))
			self.Cursor.execute(add_record, tableObject)
			self.Connection.commit()
		except mysql.connector.Error as err:
			if isinstance(table, TypeTable) and err.errno == 1062:
				pass #duplicate errors are okay for TypeTables
			elif err.errno == 1062:
				print("\n")
				print(err.errno, err.msg)
				print("Duplicate entry found -- " + str(tableObject))
				if tableName == "Order":
					self.Cursor.execute("SELECT `updated` FROM `Order` WHERE `OrderID`=%s" % (tableObject["OrderID"]))
					(updated,) = self.Cursor.fetchone()
					print("Updated:   " + str(updated))
					print("Currently: " + str(datetime.now()))
					#~ exit(1)
			else:
				print(err.errno)
				print(err.msg)
				print("---attemped statement---")
				print(add_record)
				print(tableObject)
				self.close()
				exit(1)
	
	def format_date(self, apidate):
		if apidate is not None:
			return apidate.replace("T", " ")
	
	def StoreOrders(self, orders):
		for order in orders:
			self.Store("OrderStatus", order["Status"]) #TypeTables are handled specially so this works with no modification
			
			#Populate order fields from API jsons
			Order = {
				"OrderID": order["ID"],
				"LocationID": order["LocationID"],
				"OrderNumber": order["OrderNumber"],
				"StatusID": order["Status"]["ID"],
				"DatePaid": self.format_date(order["DatePaidOffset"]),
				"DateCreated": self.format_date(order["DateCreatedOffset"]),
				"PrePaid": 0,
				"Tax": order["TotalTaxesRounded"]["Amount"]
			}
			#add together gift certificate payments to get prepaid amount
			for payment in order["Payment"]["PaymentItems"]:
				if payment["Method"]["Name"] == "Gift Certificate":
					Order["PrePaid"] -= payment["Amount"]["Amount"]
			#fix floating point rounding errors from api. Round USD to 4 decimals for convention/match SQL
			Order["PrePaid"] = round(Order["PrePaid"], 4)
			self.Store("Order", Order)
			
			#items and refunds, recursively, follow the same structure
			for item in order["Items"]:
				self.Store("ItemType", item["Type"])
				
				Item = {
					"OrderID": item["OrderID"],
					"OrderItemID": item["ID"],
					"Type": item["Type"]["ID"],
					"IsService": item["IsService"],
					"EmployeeName": item["EmployeeName"],
					"DisplayName": item["DisplayName"],
					"FinalPrice":item["DynamicPrice"]["FinalPrice"]["Amount"],
					"OriginalTagPrice":item["DynamicPrice"]["OriginalTagPrice"]["Amount"],
					"Quantity": item["Quantity"]
				}
				self.Store("Item", Item)
				
				for refund in item["Refunds"]:
					self.Store("RefundType", refund["Type"])
					self.Store("RefundPaymentMethod", refund["PaymentMethod"])
					
					Refund = {
						"OrderID": refund["OrderID"],
						"OrderItemID": refund["OrderItemID"],
						"Amount": refund["Amount"]["Amount"],
						"PaymentMethod": refund["PaymentMethod"]["ID"],
						"Type": refund["Type"]["ID"],
						"DateCreated": self.format_date(refund["DateCreatedOffset"]),
						"TotalTax": refund["TotalTax"]["Amount"]
					}
					self.Store("Refund", Refund)
	
	def GetMostRecentOrderCreatedDate(self):
		self.Cursor.execute("SELECT `DateCreated` FROM `Order` ORDER BY `DateCreated` DESC LIMIT 1")
		result = self.Cursor.fetchone()
		startDate = None
		if result is not None:
			(startDate,) = result
		return startDate
	
	#this should really be in the BlissDashboardAPI that doesn't exist yet
	def DateRangeSummary(self, fromDate, toDate):
		fromTime = datetime(fromDate.year, fromDate.month, fromDate.day, 0, 0, 0)
		toTime = datetime(toDate.year, toDate.month, toDate.day, 23, 59, 59)
		summary = {
			"Orders": None,
			"Services": None,
			"Products": None,
			"Gift Cards": None,
			"Cancel Fee": None,
			"Membership": None,
			"Adjustment": None,
			"Gross Sales": None,
			"PrePaid": None,
			"Refund": None,
			"Refund by GC": None,
			"Net Sales": None,
			"Tax": None,
			"Tips": None
		}
		fmt = '%Y-%m-%d %H:%M:%S'
		fromString = fromTime.strftime(fmt)
		toString = toTime.strftime(fmt)
		
		self.Cursor.execute("SELECT COUNT(*) FROM `Order` WHERE NOT `DatePaid` IS NULL AND `DatePaid` BETWEEN %s AND %s AND `StatusID` <>6", (fromString, toString))
		summary["Orders"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(`Item`.`OriginalTagPrice` * `Item`.`Quantity`) FROM `Item` INNER JOIN `Order` ON `Item`.`OrderID` = `Order`.`OrderID` WHERE NOT `Order`.`DatePaid` IS NULL AND `Order`.`DatePaid` BETWEEN %s AND %s AND `Order`.`StatusID` <>6 AND `Item`.`IsService`<>0", (fromString, toString))
		summary["Services"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(`Item`.`OriginalTagPrice` * `Item`.`Quantity`) AS ProductTotal FROM `Item` INNER JOIN `Order` ON `Item`.`OrderID` = `Order`.`OrderID` WHERE NOT `Order`.`DatePaid` IS NULL AND `Order`.`DatePaid` BETWEEN %s AND %s AND `Order`.`StatusID` <>6 AND `Item`.`Type`=2", (fromString, toString))
		summary["Products"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(`Item`.`OriginalTagPrice` * `Item`.`Quantity`) FROM `Item` INNER JOIN `Order` ON `Item`.`OrderID` = `Order`.`OrderID` WHERE NOT `Order`.`DatePaid` IS NULL AND `Order`.`DatePaid` BETWEEN %s AND %s AND `Order`.`StatusID` <>6 AND (`Item`.`Type`=3 OR `Item`.`Type`=12)", (fromString, toString))
		summary["Gift Cards"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(`Item`.`OriginalTagPrice` * `Item`.`Quantity`) AS CancelFee FROM `Item` INNER JOIN `Order` ON `Item`.`OrderID` = `Order`.`OrderID` WHERE NOT `Order`.`DatePaid` IS NULL AND `Order`.`DatePaid` BETWEEN %s AND %s AND `Order`.`StatusID` <>6 AND `Item`.`Type`=7", (fromString, toString))
		summary["Cancel Fee"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(`Item`.`OriginalTagPrice` * `Item`.`Quantity`) AS MembershipFee FROM `Item` INNER JOIN `Order` ON `Item`.`OrderID` = `Order`.`OrderID` WHERE NOT `Order`.`DatePaid` IS NULL AND `Order`.`DatePaid` BETWEEN %s AND %s AND `Order`.`StatusID` <>6 AND `Item`.`Type`=11", (fromString, toString))
		summary["Membership"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM((`Item`.`FinalPrice` - `Item`.`OriginalTagPrice`) * `Item`.`Quantity`) AS Adjustment FROM `Item` INNER JOIN `Order` ON `Item`.`OrderID` = `Order`.`OrderID` WHERE NOT `Order`.`DatePaid` IS NULL AND `Order`.`DatePaid` BETWEEN %s AND %s AND `Order`.`StatusID` <>6 AND `Item`.`Type`<>8", (fromString, toString))
		summary["Adjustment"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(`Item`.`FinalPrice` * `Item`.`Quantity`) AS GrossSales FROM `Item` INNER JOIN `Order` ON `Item`.`OrderID` = `Order`.`OrderID` WHERE NOT `Order`.`DatePaid` IS NULL AND `Order`.`DatePaid` BETWEEN %s AND %s AND `Order`.`StatusID` <>6 AND `Item`.`Type`<>8", (fromString, toString))
		summary["Gross Sales"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(PrePaid) FROM `Order` WHERE NOT DatePaid IS NULL AND DatePaid BETWEEN %s AND %s AND StatusID <>6", (fromString, toString))
		summary["PrePaid"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(PrePaid) FROM `Order` WHERE NOT DatePaid IS NULL AND DatePaid BETWEEN %s AND %s AND StatusID <>6", (fromString, toString))
		summary["PrePaid"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(Tax) FROM `Order` WHERE NOT DatePaid IS NULL AND DatePaid BETWEEN %s AND %s AND StatusID <>6", (fromString, toString))
		summary["Tax"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(`Item`.`FinalPrice`) AS Tips FROM `Item` INNER JOIN `Order` ON `Item`.`OrderID` = `Order`.`OrderID` WHERE NOT `Order`.`DatePaid` IS NULL AND `Order`.`DatePaid` BETWEEN %s AND %s AND `Order`.`StatusID` <>6 AND `Item`.`Type`=8", (fromString, toString))
		summary["Tips"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(TotalTax - Amount) AS Refund FROM `Refund` WHERE `DateCreated` BETWEEN %s AND %s", (fromString, toString))
		summary["Refund"] = self.Cursor.fetchall()[0][0]
		
		self.Cursor.execute("SELECT SUM(Amount - TotalTax) AS GiftCardRefund FROM `Refund` WHERE `DateCreated` BETWEEN %s AND %s AND `PaymentMethod`=2", (fromString, toString))
		summary["Refund by GC"] = self.Cursor.fetchall()[0][0]
		
		for key in summary.keys():
			if summary[key] is None:
				summary[key] = 0
		
		summary["Net Sales"] = summary["Gross Sales"] + summary["PrePaid"] + summary["Refund"] + summary["Refund by GC"]
		
		return summary
		

if __name__ == "__main__":
	from BookerAPI import BookerAPIClient
	from CalendarIterator import CalendarIterator
	def populate(dbm, locationID, pagesize, startDate = None, endDate = None):
		API = BookerAPIClient()
		if startDate is None:
			startDate = dbm.GetMostRecentOrderCreatedDate() + relativedelta(minutes=1) #add one minute to not include the most recent order
			if startDate is None:
				print("No data, please supply a starting date")
				exit(1)
			print("Starting at " + str(startDate) + " (detected)")
		else:
			print("Starting at " + str(startDate))
		if endDate is None:
			endDate = datetime.now()
		
		calendar = CalendarIterator(startDate, endDate)
		
		while calendar.segmentStart < calendar.endTime:
			resultcount = API.FindOrders(locationID, calendar.segmentStart, calendar.segmentEnd, 0, 0)["TotalResultsCount"]
			print(calendar.segmentStart.strftime("%B, %Y"), ":\n",
			API.FormatDate(calendar.segmentStart), API.FormatDate(calendar.segmentEnd), "\n",
			resultcount, "orders")
			
			numpages = int(ceil(float(resultcount)/pagesize))
			timer_start = time()
			for pagenumber in range(1, numpages + 1):
				print("Getting page %d/%d..." % (pagenumber, numpages))
				timer = time()
				
				data = API.FindOrders(locationID, calendar.segmentStart, calendar.segmentEnd, pagenumber, pagesize)
				
				if data["Results"] is None:
					if data["ErrorCode"] == 1000:
						print("Token has expired. Refreshing...")
						API.get_brand_token(True)
						API.get_location_token(locationID, True)
						data = API.FindOrders(locationID, calendar.segmentStart, calendar.segmentEnd, pagenumber, pagesize)
						if data["Results"] is None:
							print("No effect :(")
							print(data)
							exit(1)
					else:
						print(data)
						exit(1)
				dbm.StoreOrders(data["Results"])
				
				#---time estimation---
				timer_end = time()
				elapsed = timer_end - timer_start
				total_time = elapsed  / (float(pagenumber) / numpages)
				estimated_time = total_time - elapsed
				print("Last request: %.2fs\t Estimated time: %s" % (timer_end - timer, str(timedelta(seconds=round(estimated_time)))))
			#increment month
			calendar.iterate()
	
	dbm = DatabaseManager(user = "ella-bliss", password = "T3nuispinus!", host = "localhost", database = "ELLABLISS")
	
	#~ dbm.nuke_db()
	#~ dbm.init_tables()
	
	populate(dbm, 30234, 50)#, datetime(2016, 9, 1, 0, 0, 0))#, datetime(2015, 9, 30, 23, 59, 59))
	
	calendar = CalendarIterator(date(2020, 2, 19), date(2020, 2, 19))
	while calendar.segmentStart < calendar.endTime:
		print(calendar.segmentStart, calendar.segmentEnd)
		print("\n".join(["\t-{}: ${:,.2f}".format(*i) if not isinstance(i[1], int) else "\t-{}: {}".format(*i) for i in dbm.DateRangeSummary(calendar.segmentStart, calendar.segmentEnd).items()]))
		calendar.iterate()
	print(calendar.startTime, calendar.endTime)
	print("Orders: {Orders} Services: {Services} Products: {Products} Sales: {Net Sales}".format(**dbm.DateRangeSummary(calendar.startTime, calendar.endTime))
	#~ print("\n".join(["\t-{}: ${:,.2f}".format(*i) if not isinstance(i[1], int) else "\t-{}: {}".format(*i) for i in dbm.DateRangeSummary(calendar.startTime, calendar.endTime).items()]))
	dbm.close()
