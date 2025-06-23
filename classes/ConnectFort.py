import pymssql
import config.config  as config

class ConnectFort:
    def __init__(self):
        self.username = config.MSSQLLOGIN
        self.password = config.MSSQLPASSWORD
        self.host = config.MSSQLHOST
        self.port = config.MSSQLPORT  # Optional, default is usually 1433
        self.database = config.MSSQLDB  # Specify the database name
        self.connection = None

    def connect(self):
        try:
            self.connection = pymssql.connect(
                server=self.host,
                user=self.username,
                password=self.password,
                database=self.database
            )
            return self.connection
        except pymssql.DatabaseError as e:
            print(f"Error connecting to MSSQL Database: {e}")
            return None

    def close(self):
        if self.connection:
            self.connection.close()