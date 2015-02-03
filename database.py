import sqlite3

class MetDatabase(object):
  """Sqlite database abstraction for Met data grabber."""
  def __init__(self, filename):
    """Initiate connection to Sqlite database.
    
    Keyword arguments:
    filename -- the name of the database file to open
    """
    
    self.connect(filename)

  def connect(self, filename):
    """Connect to Sqlite database.
    
    Arguments:
    filename -- the name of the database file to open
    """
    
    self.db = sqlite3.connect(filename)

  @staticmethod
  def create(filename):
    """Create a Sqlite database, populate it with a storage table, and return a new MetDatabase object with this database.
    
    Arguments:
    filename -- the name of the database file to create
    """
    
    db = sqlite3.connect(filename)
    cursor = db.cursor()
    
    # create Met table
    cursor.execute('CREATE TABLE met (timestamp INTEGER, windDirection TEXT, windSpeed INTEGER, windGust INTEGER, visibility INTEGER, screenTemperature REAL, pressure INTEGER, PRIMARY KEY (timestamp))')
    
    db.commit()
    cursor.close()
    
    return MetDatabase(filename)
  
  def insert(self, row):
    """Insert row of data into the database table.
    
    Arguments:
    row -- a dict containing the columns and their associated values to insert
    """
    
    cursor = self.db.cursor()
    
    cursor.execute('INSERT INTO met VALUES (:timestamp, :windDirection, :windSpeed, :windGust, :visibility, :screenTemperature, :pressure)', row)
    
    self.db.commit()
  
  def execute(self, query, *args, **kwargs):
    """Execute database query.
    
    Arguments:
    query -- the SQL query to pass to Sqlite
    
    Other arguments: anything that sqlite's cursor execute method supports
    """
    
    cursor = self.db.cursor()
    
    return cursor.execute(query, *args, **kwargs)