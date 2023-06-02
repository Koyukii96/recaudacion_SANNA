import pyodbc
from sqlalchemy import MetaData, create_engine


#sqlalchemy class connection
class SQLAlchemyConnection:

    def __init__(self, server, database,fast=True):
        self.server = server
        self.database = database
        self.connection = None
        self.engine = None
        self.metadata = None
        self.fast = fast
   
    def connect(self):

              
       # self.connection = pyodbc.connect(
       #     'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server +
       #     ';DATABASE=' + self.database + ';Trusted_Connection=yes')

       #self.connection = pyodbc.connect(
       #     'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server +
       #     ';DATABASE=' + self.database + ';UID=sa;PWD=abc123')
        params = 'DRIVER={ODBC Driver 17 for SQL Server};' \
         'SERVER=localhost;' \
         'PORT=1433;' \
         'DATABASE='+self.database+';' \
         'UID=sa;' \
         'PWD=Clave123;'

       
        self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
                            
        self.metadata = MetaData(self.engine)

    def execute(self, query):
        return self.engine.execute(query)

    def close(self):
        #self.connection.close()
        self.engine.dispose()
        #self.metadata.dispose()
        #self.connection = None
        self.engine = None
        self.metadata = None