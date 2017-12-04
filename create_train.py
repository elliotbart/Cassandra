# -*- coding: utf-8 -*-
"""
Created on Fri May 19 10:58:28 2017

@author: nf26p006
"""
from cassandra.cluster import Cluster

cluster = Cluster()

session = cluster.connect('e28')

'''
session.execute("""
DROP TABLE trip_taxis;
""")
session.execute("""
DROP TABLE trip_distances;
""")
session.execute("""
DROP TABLE trip_call_types;
""")
session.execute("""
DROP TABLE trip_origin_stands;
""")
session.execute("""
DROP TABLE trip_origin_calls;
""")
session.execute("""
DROP TABLE trip_years;
""")
session.execute("""
DROP TABLE trip_months;
""")
session.execute("""
DROP TABLE trip_days;
""")
session.execute("""
DROP TABLE trip_hours;
""")
session.execute("""
DROP TABLE trip_arrivals;
""")
session.execute("""
DROP TABLE trip_departures;
""")


session.execute("""
CREATE TABLE IF NOT EXISTS trip_taxis (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(taxi_id, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_distances (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(dist, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_call_types (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(call_type, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_origin_stands (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(origin_stand, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_origin_calls (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(origin_call, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_hours (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(hour, year, month, day, min, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_days (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key((day, month, year), hour, min, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_months (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(month, year, day, hour, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_years (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(year, month, day, hour, min, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_departures (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(starting_point, ending_point, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS trip_arrivals (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key(ending_point, starting_point, trip_id))
""")



session.execute("""
CREATE TABLE IF NOT EXISTS trip_duration (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, duration double, primary key(duration, trip_id))
""")
'''

session.execute("""
CREATE TABLE IF NOT EXISTS trip_day_types (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, duration double, primary key(daytype, trip_id))
""")