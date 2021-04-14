import datetime
from dateutil.relativedelta import *

class CalendarIterator:
	startTime = None
	endTime = None
	segmentStart = None
	segmentEnd = None
	def __init__(self, startTime, endTime):
		self.startTime = startTime
		self.endTime = endTime
		self.segmentStart = startTime
		self.calc_segment_end()
	
	def calc_segment_end(self):
		if isinstance(self.segmentStart, datetime.datetime):
			self.segmentEnd = (self.segmentStart.replace(day=1,hour=0,minute=0,second=0)+relativedelta(months=+1))+relativedelta(days=-1)
		elif isinstance(self.segmentStart, datetime.date):
			self.segmentEnd = (self.segmentStart.replace(day=1)+relativedelta(months=+1))+relativedelta(days=-1)
		else:
			print(type(segmentStart))
			exit(1)
		if self.segmentEnd > self.endTime:
			self.segmentEnd = self.endTime
	
	def iterate(self):
		if isinstance(self.segmentStart, datetime.datetime):
			self.segmentStart = self.segmentStart.replace(day=1,hour=0,minute=0,second=0)+relativedelta(months=+1)
		elif isinstance(self.segmentStart, datetime.date):
			self.segmentStart = self.segmentStart.replace(day=1)+relativedelta(months=+1)
		self.calc_segment_end()
