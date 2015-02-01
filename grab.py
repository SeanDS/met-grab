import requests
import requests.exceptions
from bs4 import BeautifulSoup

timeout = 5

payload = {'Type': 'ThreeHourly', 'PredictionSiteId': '310009', 'Date': '15/01/2015', 'PredictionTime': '0000'}

urlResponse = None

while True:
  try:
    urlResponse = requests.post('http://datagovuk.cloudapp.net/query', data=payload, timeout=timeout)
    
    break
  except requests.exceptions.Timeout:
    print "Connect timeout. Trying again..."
  except requests.exceptions.ConnectionError as e:
    print "Connect error. Failing..."
    
    raise e

soup = BeautifulSoup(urlResponse.text)

downloadUrl = None

for thisLink in soup.find_all('a'):
  thisUrl = thisLink.get('href')
  
  if thisUrl[-3:] == 'csv':
    # found CSV download link
    downloadUrl = thisUrl
    
    break

csvResponse = None

with open('file.csv', 'wb') as handle:
  while True:
    try:
      csvResponse = requests.get(downloadUrl, stream=True)
      
      break
    except requests.exceptions.ConnectTimeout:
      print "Connect timeout. Trying again..."

  if not csvResponse.ok:
    print "Something went wrong"

  for block in csvResponse.iter_content(1024):
    if not block:
      break

    handle.write(block)

print "Done"