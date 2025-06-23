import teradatasql
import config.config  as config

class ConnectDwh:
    def __init__(self):
        self.username = config.DWH_LOGIN
        self.password = config.DWH_PASSWORD
        self.host = config.DWH_HOST 
        self.connection = None

    def connect(self):

        try:
            with teradatasql.connect(host=self.host, user=self.username, password=self.password) as conn:
                self.connection = conn                
                return self.connection
        except Exception as e:
            print(f"Error connecting to DWH Database: : {e}")
            return None


    def close(self):
        if self.connection:
            self.connection.close()