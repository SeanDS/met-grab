from __future__ import division

import numpy
import matplotlib.pyplot as plt
import matplotlib.dates as md

import datetime

import database

# connect to database
db = database.MetDatabase('glasgow.db')

# get data
rows = db.execute('SELECT * FROM met ORDER BY timestamp ASC')

# convert data to numpy array
data = numpy.array(rows.fetchall())

# convert first column to datetime objects
dates = [datetime.datetime.fromtimestamp(int(x)) for x in data[:, 0]]

# rotate ticks on x-axis
plt.xticks(rotation=25)

# labels
plt.xlabel('Date')
plt.ylabel('Temperature')
plt.title('Temperature')

# grid
plt.grid()

# plot
plt.plot(dates, data[:, 5])

# adjust layout to fit everything in
plt.tight_layout()

# show
plt.show()