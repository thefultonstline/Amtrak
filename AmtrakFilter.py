import pandas as pd
import sqlite3
import geopandas as gpd
import utm

#function for generating updated GTFS files
def output(filename, rows): 
    
    with open(filename + '.txt', mode='w') as file: 
        column_names = [description[0] for description in cursor.description]
        file.write(','.join(column_names) + '\n')
    
        for row in rows:
            file.write(','.join(map(str, row)) + '\n')

 #connect to a database for data to be stored
db_connection = sqlite3.connect('gtfs_database.db')

#store route data in  the database
routestxt = pd.read_csv('USGTFS/routes.txt')

routes_schema = """
CREATE TABLE IF NOT EXISTS routes (
    route_id TEXT PRIMARY KEY,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_desc TEXT,
    route_type INTEGER
);
"""

db_connection.execute(routes_schema)

#filter out routes that we are not concerned with
cursor = db_connection.cursor()

cursor.execute(f'DELETE FROM routes')

routestxt.to_sql('routes', db_connection, if_exists='replace', index=False)

condition = "route_long_name NOT IN ('Lake Shore Limited','Empire Service','Adirondack','Maple Leaf','Ethan Allen Express')"
cursor.execute(f'DELETE FROM routes WHERE {condition}')

db_connection.commit()

#add trips to database
tripstxt = pd.read_csv('USGTFS/trips.txt')

trips_schema = """
CREATE TABLE IF NOT EXISTS trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT PRIMARY KEY,
    trip_headsign TEXT,
    direction_id INTEGER,
    block_id TEXT
);
"""

db_connection.execute(trips_schema)


cursor.execute(f'DELETE FROM trips')

tripstxt.to_sql('trips', db_connection, if_exists='replace', index=False)

#remove all trips for routes that are not in database
delete_query = f'''
    DELETE FROM trips
    WHERE route_id NOT IN (SELECT route_id FROM routes)
'''
cursor.execute(delete_query)

db_connection.commit()   

#generate updated trips file SHOULD BE MOVED TO BOTTOM
cursor.execute(f'SELECT * FROM trips')
rows = cursor.fetchall()
output('trips', rows)#generates updated

#add stops times data to database
stoptimestxt = pd.read_csv('USGTFS/stop_times.txt')

stoptimes_schema = """
CREATE TABLE IF NOT EXISTS stoptimes (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INTEGER,
    stop_headsign TEXT,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    shape_dist_traveled REAL,
    timepoint INTEGER
);
"""
#remove irrelevant stop time data
db_connection.execute(stoptimes_schema)
cursor.execute(f'DELETE FROM stoptimes')

stoptimestxt.to_sql('stoptimes', db_connection, if_exists='replace', index=False)

delete_query = f'''
    DELETE FROM stoptimes
    WHERE trip_id NOT IN (SELECT trip_id FROM trips)
'''
cursor.execute(delete_query)
db_connection.commit() 

#add stop data to database
stopstxt = pd.read_csv('USGTFS/stops.txt')

stops_schema = """
CREATE TABLE IF NOT EXISTS stops (
    stop_id TEXT PRIMARY KEY,
    stop_name TEXT,
    stop_desc TEXT,
    stop_lat REAL,
    stop_lon REAL,
    zone_id TEXT,
    stop_url TEXT,
    location_type INTEGER,
    parent_station TEXT
);
"""
#filter out stops that are not in use
db_connection.execute(stops_schema)
cursor.execute(f'DELETE FROM stops')

stopstxt.to_sql('stops', db_connection, if_exists='replace', index=False)

delete_query = f'''
    DELETE FROM stops
    WHERE stop_id NOT IN (SELECT stop_id FROM stoptimes)
'''
cursor.execute(delete_query)
db_connection.commit() 

#create a table for all the GIS data and which
cursor.execute(f'DROP TABLE IF EXISTS gis')
db_connection.commit() 

gis_schema = """
CREATE TABLE IF NOT EXISTS gis (
    count INTEGER PRIMARY KEY,
    segment INTEGER,
    lat TEXT,
    long TEXT
);
"""
db_connection.execute(gis_schema)
db_connection.commit() 

#read the shp file
gisdata = gpd.read_file('Amtrakgis/AMTRAK.shp')

multilines = [0, 2, 22, 24, 29]#the shp has lines and multlines that contain lines. Out of 34 segments, 5 are multilines

insert_query = """
INSERT INTO gis (count, segment, lat, long)
VALUES (?, ?, ?, ?);
"""

#loop through all segments and extract all coordinates, convert each segment from WGS84 to lat/long, and store segment data in database
count = 0
for i in range(0,35):
    count += 1
    if i not in multilines:
        for ii in range(0,len(gisdata['geometry'][i].coords)):
            data = gisdata['geometry'][i].coords[ii]
            
    else:
        for ii in range(0,len(gisdata['geometry'][i].geoms)):
            for iii in range(0, len(gisdata['geometry'][i].geoms[ii].coords)):
                data = gisdata['geometry'][i].geoms[ii].coords[iii]

    conversion = utm.to_latlon(data[0], data[1], 18, 'U')       
    record_values = (count, i, data[0], data[1])
    cursor.execute(insert_query, record_values)
    db_connection.commit()


#create a map in the database. Each GIS segment is assigned to a station. 
cursor.execute(f'DROP TABLE IF EXISTS map')
db_connection.commit() 

map_schema = """
CREATE TABLE IF NOT EXISTS map (
    stopid TEXT PRIMARY KEY,
    rsegment INTEGER,
    rsegment2 INTEGER,
    lsegment INTEGER,
    lsegment2 INTEGER,
    rstop TEXT,
    lstop TEXT,
    bstop TEXT
);
"""  
db_connection.execute(map_schema)
db_connection.commit()  

def insertmap(stopid, rsegment, rsegment2, lsegment, lsegment2, rstop, lstop, bstop):
    insert_query = """
    INSERT INTO map (stopid, rsegment, rsegment2, lsegment, lsegment2, rstop, lstop, bstop)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    record_values = (stopid, rsegment, rsegment2, lsegment, lsegment2, rstop, lstop, bstop)
    cursor.execute(insert_query, record_values)
    db_connection.commit()

#hardcode the map in the database with current stop, right segments, left segments, next stop on right, nex stop on left, and previous stop
insertmap('NYP', 22, None, None, None, 'YNY', None, None)
insertmap('YNY', 21, None, None, None,'CRT', None, 'NYP')
insertmap('CRT', 1, None, None, None, 'POU', None, 'YNY')
insertmap('POU', 20, None, None, None,'RHI', None, 'CRT')
insertmap('RHI', 19, None, None, None, 'HUD', None, 'POU')
insertmap('HUD', 18, None, None, None, 'ALB', None,'RHI')
insertmap('ALB', 17, None, None,  None, 'SDY', None, 'HUD')
insertmap('SDY', 16, None, 3, None, 'SAR', 'AMS', 'ALB')
insertmap('SAR', 31, None, None, None, 'FED', None, 'SDY')
insertmap('FED', None, None, 5, 34, 'WHL', 'CNV', 'SAR')
insertmap('WHL', 30, None, None, None, 'FTC', None, 'FED')
insertmap('FTC', 29, None, None, None, 'POH', None, 'WHL')
insertmap('POH', 28, None, None, None, 'WSP', None, 'FTC')
insertmap('WSP', 27, 26, None, None, 'PLB', None, 'POH')
insertmap('PLB', 25, None, None, None, 'RSP', None, 'WSP')
insertmap('RSP', 32, None, None, None, 'SLQ', None, 'PLB')
insertmap('AMS', 15, None, None, None, 'UCA', None, 'SDY')
insertmap('UCA', 14, None, None, None, 'ROM', None, 'AMS')
insertmap('ROM', 13, None, None, None, 'SYR', None, 'UCA')
insertmap('SYR', 12, 11, None, None, 'ROC', None, 'ROM')
insertmap('ROC', 10, None, None, None, 'BUF', None, 'SYR')
insertmap('BUF', 9, 8, None, None, 'BFX', None, 'ROC')
insertmap('BFX', 7, None, None, None, None, None, 'BFX')

insertmap('SLQ', None, None, None, None, 'MTR', None, None)
insertmap('MTR', None, None, None, None, None, None, 'SLQ')

#delete stop times from the GTFS for stops that are not in this map (some trains may go to Boston instead of NYP)
delete_query = f'''
    DELETE FROM stoptimes
    WHERE stop_id NOT IN (SELECT stopid FROM map)
'''
cursor.execute(delete_query)
db_connection.commit() 

cursor.execute(f'SELECT trip_id FROM trips')
trips = cursor.fetchall()

#create place for shape data in database
cursor.execute(f'DROP TABLE IF EXISTS shapes')
db_connection.commit() 

#what is b again?
shapes_schema = """
CREATE TABLE IF NOT EXISTS map (
    count INTEGER PRIMARY KEY,
    shape_id INTEGER,
    shape_pt_lat TEXT,
    shape_pt_long TEXT,
    b INTEGER
);
"""  
db_connection.execute(shapes_schema)
db_connection.commit()  

#create shape data #NOT WORKING PROPERLY
count = 0
shape_id = 0
#loop through all trips in the database
for i in range(0,len(trips)):
    cursor.execute(f'SELECT stop_id, stop_sequence FROM stoptimes WHERE trip_id = ?', (trips[i][0],))
    stoptimes = cursor.fetchall()
    cursor.execute(f'SELECT direction_id FROM trips WHERE trip_id = ?', (trips[i][0],))
    direction = cursor.fetchall()

    #reverse the stoptimes in for inbound trips so the map is traversed in one direction
    if direction[0][0] == 1:#inbound
        stoptimes = stoptimes[::-1]
 #Generate the order of stops that will be visited (INCLUDES STOPS THAT ARE NOT SERVICED)
    stopsequence = []
    for ii in range(len(stoptimes)-1, -1, -1):#loop from the last stop of the station going towards New York Penn
        cursor.execute(f'SELECT * FROM map WHERE stopid = ?', (stoptimes[ii][0],))
        stop = cursor.fetchone()
        stopsequence.insert(0, stop)
    print(stopsequence)
    #problem is stops that are skipped are not included
'''   shapesequence = []
    for ii in range(0,len(stopsequence)-1):
        print(stopsequence[ii+1][0])
        diverge = stopsequence[ii].index(stopsequence[ii+1][0])
        print(diverge)
'''

#generate updated GTFS files
cursor.execute(f'SELECT * FROM routes')
rows = cursor.fetchall()
output('routes', rows)

cursor.execute(f'SELECT * FROM stoptimes')
rows = cursor.fetchall()
output('stop_times', rows)

cursor.execute(f'SELECT * FROM stops')
rows = cursor.fetchall()
output('stops', rows)

cursor.close()
db_connection.close()
print("And Done")