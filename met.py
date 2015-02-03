import datetime
import requests
import requests.exceptions
from bs4 import BeautifulSoup

class Grabber(object):
  """Object to handle grabbing of CSV data from the Met Office API."""
  
  __queryUrl = 'http://datagovuk.cloudapp.net/query'
  
  def __init__(self, timeout=5, retries=5):
    """Instantiate grabber object.
    
    Arguments:
    timeout -- time in seconds to attempt a retrieval of data from the Met Office website before aborting
    retries -- number of times to try again after an initial timeout
    """
    
    self.timeout = timeout
    self.retries = retries
  
  @property
  def timeout(self):    
    return self.__timeout
  
  @timeout.setter
  def timeout(self, timeout):
    self.__timeout = timeout
    
  @property
  def retries(self):
    return self.__retries
  
  @retries.setter
  def retries(self, retries):
    self.__retries = retries

  def grab(self, filename, site, date, time):
    """Grab a specific CSV file for a specific site, at a specific time.
    
    Arguments:
    filename -- path to store the grabbed CSV file
    site -- the site ID to download data for
    date -- the date to download data for
    time -- the time to download data for
    """
    
    urlResponse = self.getCsvDownloadPage(site, date, time)
    csvUrl = self.getCsvUrl(urlResponse)

    csvResponse = None

    with open(filename, 'wb') as fileHandle:
      while True:
	try:
	  csvResponse = requests.get(csvUrl, stream=True)
	  
	  break
	except requests.exceptions.ConnectTimeout:
	  print "Connect timeout. Trying again..."

      if not csvResponse.ok:
	raise Exception('Something went wrong')

      for block in csvResponse.iter_content(1024):
	if not block:
	  break

	fileHandle.write(block)
	
  def getCsvDownloadPage(self, site, date, time):
    """Get CSV download page for a site at a specific date and time.
    
    Arguments:
    site -- the site ID to get the CSV download page for
    date -- the date to get the CSV download page for
    time -- the time to get the CSV download page for
    """
    
    payload = {'Type': 'Observation', 'ObservationSiteID': site, 'Date': date, 'PredictionTime': time}

    urlResponse = None
    tries = 0

    while True:
      try:
	tries += 1
	
	urlResponse = requests.post(self.__queryUrl, data=payload, timeout=self.timeout)
	
	break
      except requests.exceptions.Timeout:
	if tries >= self.retries:
	  raise Exception('Multiple connection timeouts. Number of retries exceeded limit.')
	else:
	  print "Connect timeout. Trying again ({0} of {1})...".format(tries + 1, self.retries)

    return urlResponse
	
  def getCsvUrl(self, urlResponse):
    """Get CSV download URL from an HTTP response after requesting a download page.
    
    Arguments:
    urlResponse -- the Requests module object containing the HTTP response from the CSV download page
    """
    
    # parse HTML document
    soup = BeautifulSoup(urlResponse.text)

    csvUrl = None

    if soup.find(text='No matching records were found.') is not None:
      raise NoRecordException('No records found.')

    for thisLink in soup.find_all('a'):
      thisUrl = thisLink.get('href')
      
      if thisUrl[-3:] == 'csv':
	# found CSV download link
	csvUrl = thisUrl
	
	break
    
    if csvUrl is None:
      raise Exception('Couldn\'t find CSV link in downloaded web page.')
    
    return csvUrl
  
  def getDateList(self, days):
    """Helper method to return a list of Met-compatible dates from a range of days.
    
    Arguments:
    days -- dates to generate in the form of a list of days with respect to today (so yesterday is -1, last week is -7, etc.)
    """
    today = datetime.datetime.today()
    
    return [(today - datetime.timedelta(days=day)).strftime('%d/%m/%Y') for day in days]

  def getTimeList(self, hours):
    """Helper method to return a list of Met-compatible times from a range of hours.
    
    Arguments:
    hours -- hours to generate in the form of a list of numbers between 0 and 23, corresponding to the hour of day
    """
    times = []

    for i in hours:
      times.append('%02d00' % i)
    
    return times

class NoRecordException(Exception):
  pass