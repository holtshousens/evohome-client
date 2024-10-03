from evohomeclient2 import EvohomeClient;
import pyodbc;
from datetime import datetime;
import time;
import configparser;

projectLocation = "C:\\Users\\holts\\OneDrive\\Documents\\GitHub\\evohome-client\\";

configPath = projectLocation + "config.ini";

print("retrieve config file" + configPath)

config = configparser.ConfigParser()

config.read(configPath)

databasename = config.get('DB', 'dbname');
server = config.get('DB', 'dbserver');
driver = config.get('DB', 'dbdriver'); 

username = config.get('evohome', 'username');
password = config.get('evohome', 'password');

CONNECTION_STRING = 'DRIVER=' + driver + '; SERVER=' + server + '; DATABASE=' + databasename + '; Trusted_Connection=yes';

def insert_zones(thermostat, id, name, temp, setpoint):
    cursor = connection.cursor();
    sSQL = '''
        INSERT INTO 
        dbo.Zones (uid, timestamp, thermostat, id, [name], temp, setpoint) 
        VALUES (NEWID(), CURRENT_TIMESTAMP, \'''' + thermostat + '''\', \'''' + id + '''\', \'''' + name + '''\', \'''' + str(temp) + '''\', \'''' + str(setpoint) + '''\')
    '''

    cursor.execute(sSQL);
    
    print('id: ' + id + ' inserted')
    #logging.info('id: ' + id + ' inserted')

    cursor.commit();

# Infinite loop every 5 minutes, send temperatures to sql
while True:
    connection = pyodbc.connect(CONNECTION_STRING);

    try:
        client = EvohomeClient(username, password, debug=True)

        for device in client.temperatures():
            print(device)
            insert_zones(thermostat=device['thermostat'], id=device['id'], name=device['name'], temp=device['temp'], setpoint=device['setpoint'])

        connection.close();
    except:
        print("Error when connecting to internet");
        #logging.error("Error when connecting to internet");

        connection.close();

    print ("Going to sleep for 5 minutes");
    #logging.info("Error when connecting to internet");

    time.sleep(300)