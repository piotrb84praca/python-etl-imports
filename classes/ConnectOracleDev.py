import cx_Oracle
import config.config  as config

class ConnectOracleDev:
    def __init__(self):
        self.username = config.ORACLE_DEV_LOGIN
        self.password = config.ORACLE_DEV_PASSWORD
        self.host = config.ORACLE_DEV_HOST
        self.port = config.ORACLE_DEV_PORT
        self.sid = config.ORACLE_DEV_SID
        self.connection = None

    def connect(self):
        try:
            dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.sid)
            self.connection = cx_Oracle.connect(self.username, self.password, dsn)
            return self.connection
        except cx_Oracle.DatabaseError as e:
            print(f"Error connecting to Oracle Dev Database: {e}")
            return None

    def close(self):
        if self.connection:
            self.connection.close()