from classes.ConnectDwh import ConnectDwh

try:
    conn = ConnectDwh()
    connection = conn.connect()
    #Example connection Make query here
    print("Connection successful!")
    
except Exception as e:
    print(f"Error: {e}")