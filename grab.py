from __future__ import division

import csv
import time
import datetime
import pytz

import met
import database

grabber = met.Grabber()
#db = database.MetDatabase.create('glasgow.db')
db = database.MetDatabase('glasgow.db')

site = 3134
filename = 'glasgow.csv'

dates = grabber.getDateList(range(215, 0, -1))
times = grabber.getTimeList(range(0, 24))

skipped = []
count = 0
total = len(dates) * len(times)

for thisDate in dates:
  for thisTime in times:
    count += 1
    
    print "Getting {0} {1} ({2} of {3})".format(thisDate, thisTime, count, total)
    
    try:
      grabber.grab(filename=filename, site=site, date=thisDate, time=thisTime)
    except met.NoRecordException:
      print "No records found for {0} {1}. Skipping.".format(thisDate, thisTime)
      
      skipped.append((thisDate, thisTime))
      
      continue

    with open(filename, 'rb') as csvFile:
      reader = csv.reader(csvFile, delimiter=',')
      
      # skip header
      next(reader)
      
      for row in reader:
	thisDateTime = datetime.datetime.strptime('{0} {1}'.format(row[6], row[5]), '%Y-%m-%d %H:%M')
	
	# localise
	thisDateTime = thisDateTime.replace(tzinfo=pytz.timezone('Europe/London'))
	
	# convert to timestamp
	timestamp = time.mktime(thisDateTime.timetuple())
	
	# gust should be non-zero
	if row[9] == '':
	  row[9] = 0
	
	try:
	  db.insert({'timestamp': timestamp, 'windDirection': row[7], 'windSpeed': row[8], 'windGust': row[9], 'visibility': row[10], 'screenTemperature': row[11], 'pressure': row[12]})
	except Exception, e:
	  print e
	  
	  continue

if len(skipped) > 0:
  print "\nSkipped:"

  for i in skipped:
    print "{0} {1}".format(i[0], i[1])

print "\nDone"