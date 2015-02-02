import sqlite3

class MetDatabase(object):  
  def __init__(self, filename):
    self.connect(filename)

  def connect(self, filename):
    self.db = sqlite3.connect(filename)

  @staticmethod
  def create(filename):
    db = sqlite3.connect(filename)
    cursor = db.cursor()
    
    cursor.execute('CREATE TABLE met (timestamp INTEGER, windDirection TEXT, windSpeed INTEGER, windGust INTEGER, visibility INTEGER, screenTemperature REAL, pressure INTEGER, PRIMARY KEY (timestamp))')
    
    db.commit()
    cursor.close()
    
    return MetDatabase(filename)
  
  def insert(self, row):
    cursor = self.db.cursor()
    
    cursor.execute('INSERT INTO met VALUES (:timestamp, :windDirection, :windSpeed, :windGust, :visibility, :screenTemperature, :pressure)', row)
    
    self.db.commit()
  
  def execute(self, query, *args, **kwargs):
    cursor = self.db.cursor()
    
    return cursor.execute(query, *args, **kwargs)