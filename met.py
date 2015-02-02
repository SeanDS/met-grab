import datetime
import requests
import requests.exceptions
from bs4 import BeautifulSoup

class Grabber(object):
  __queryUrl = 'http://datagovuk.cloudapp.net/query'
  
  def __init__(self, timeout=5, retries=5):
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
    today = datetime.datetime.today()
    
    return [(today - datetime.timedelta(days=day)).strftime('%d/%m/%Y') for day in days]

  def getTimeList(self, hours):
    times = []

    for i in hours:
      times.append('%02d00' % i)
    
    return times

class NoRecordException(Exception):
  pass