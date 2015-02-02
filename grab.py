from __future__ import division

import csv
import time
import datetime

import met
import database

grabber = met.Grabber()
db = database.MetDatabase.create('glasgow.db')

site = 3134
filename = 'glasgow.csv'

dates = grabber.getDateList(range(31, 0, -1))
times = grabber.getTimeList(range(0, 24))

for thisDate in dates:
  for thisTime in times:
    print "Getting {0} {1}".format(thisDate, thisTime)
    
    try:
      grabber.grab(filename=filename, site=site, date=thisDate, time=thisTime)
    except met.NoRecordException:
      print "No records found for {0} {1}. Skipping.".format(thisDate, thisTime)
      
      continue

    with open(filename, 'rb') as csvFile:
      reader = csv.reader(csvFile, delimiter=',')
      
      # skip header
      next(reader)
      
      for row in reader:
	timestamp = time.mktime(datetime.datetime.strptime('{0} {1}'.format(row[6], row[5]), '%Y-%m-%d %H:%M').timetuple())
	
	# gust should be non-zero
	if row[9] == '':
	  row[9] = 0
	
	db.insert({'timestamp': timestamp, 'windDirection': row[7], 'windSpeed': row[8], 'windGust': row[9], 'visibility': row[10], 'screenTemperature': row[11], 'pressure': row[12]})