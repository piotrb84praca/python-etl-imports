import cx_Oracle
import config.config  as config
from sqlalchemy import create_engine, text

class ConnectOracleDevEngine:
    def __init__(self):
        self.username = config.ORACLE_DEV_LOGIN
        self.password = config.ORACLE_DEV_PASSWORD
        self.host = config.ORACLE_DEV_HOST
        self.port = config.ORACLE_DEV_PORT
        self.sid = config.ORACLE_DEV_SID
        self.connection = None

    def connect(self):
        try:
            oracle_engine = create_engine(f'oracle+cx_oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.sid}')
            self.connection = oracle_engine.connect()
            return self.connection
        except cx_Oracle.DatabaseError as e:
            print(f"Error connecting to Oracle Dev Database: {e}")
            return None
            
    def get_cursor(self):
        if self.connection:
            return self.connection.connection.cursor()
        else:
            print("No active connection.")
            return None
            
    def close(self):
        if self.connection:
            self.connection.close()