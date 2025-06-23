import logging
import os
from datetime import datetime

class ImportLogger:
    def __init__(self):

        date_str = datetime.now().strftime("%Y-%m-%d")
        log_file = f'logs/import_log_{date_str}.txt'        
     
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'  
        )
        self.start_time = None
        self.end_time = None
        self.rows_affected = 0
        self.import_name = '----'

    def start_import(self,import_name):
        self.import_name = import_name
        self.start_time = datetime.now()
        logging.info(f"Import {self.import_name} started: {self.start_time}")
        

    def end_import(self, rows_affected):
        self.end_time = datetime.now()
        self.rows_affected = rows_affected
        duration = self.end_time - self.start_time
        logging.info(f"Import {self.import_name} ended: {self.end_time} Rows affected: {self.rows_affected}. Duration: {duration}.")

    def add_to_import(self, msg):      
        logging.info(f"Import {self.import_name} msg: {msg}")
        
    def log_error(self, error_message):
        logging.error(f"Error occurred: {error_message}")