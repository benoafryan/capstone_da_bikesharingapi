import sqlite3
from unittest import result
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

def insert_into_stations(station_data, conn):
    query = f"""INSERT INTO stations values {station_data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()
    
def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/trips/totalrent') 
def route_yearly_trips():
    conn = make_connection()
    trips = get_yearly_trips(conn)
    return trips.to_json()
    
def get_yearly_trips(conn):
    query = f"""SELECT strftime('%Y', REPLACE(start_time, ' UTC', '')) as year, 
                COUNT(bikeid) as total_rent, 
                AVG(duration_minutes) as average_rent_duration
                FROM trips
                GROUP BY year
                """
    result = pd.read_sql_query(query, conn)
    return result.set_index('year')



@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

def insert_into_trips(trip_data, conn):
    query = f"""INSERT INTO trips values {trip_data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

@app.route('/trips/totalrent/station_timely') 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    specified_date = req['period']
    conn = make_connection()
    station = select_station(specified_date, conn)
    return station.to_json()

def select_station(specified_date,conn):

    query = f"SELECT * FROM trips WHERE start_time LIKE '{specified_date}%'"
    selected_data = pd.read_sql_query(query, conn)
    result = selected_data.groupby('start_station_id').agg({
    'bikeid' : 'count', 
    'duration_minutes' : 'mean' })
    return result


if __name__ == '__main__':
    app.run(debug=True, port=5000)