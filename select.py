# -*- coding: utf-8 -*-
"""
Created on Fri May 19 10:58:28 2017

@author: nf26p006
"""

import csv
import datetime
import numpy as np

from cassandra.cluster import Cluster

cluster = Cluster()

session = cluster.connect('e28')
session.default_timeout = 9999

nb_rows = 0
requeteA = "SELECT COUNT(call_type) FROM e28.trip_call_types WHERE call_type='A';"
rows = session.execute(requeteA)

for row in rows:
	print("Calls_types_count A :")
	print(row)

requeteB = "SELECT COUNT(call_type) FROM e28.trip_call_types WHERE call_type='B';"
rows = session.execute(requeteB)

for row in rows:
	print("Calls_types_count B :")
	print(row)
	
requeteC = "SELECT COUNT(call_type) FROM e28.trip_call_types WHERE call_type='C';"
rows = session.execute(requeteC)


for row in rows:
	print("Calls_types_count C:")
	print(row)

requete = "SELECT origin_stand, count(origin_stand) FROM e28.trip_origin_stands GROUP BY origin_stand;"
rows = session.execute(requete)

for row in rows:
	print("Origin_stands_count C:")
	print(row)




requete = "SELECT hour, count(hour) FROM e28.trip_hours GROUP BY hour;"
rows = session.execute(requete)

for row in rows:
	print("Hours_count:")
	print(row)


requete = "SELECT max(dist) FROM e28.trip_distances;"
rows = session.execute(requete)

for row in rows:
	print("Max distance:")
	print(row)

requete = "SELECT min(dist) FROM e28.trip_distances;"
rows = session.execute(requete)

for row in rows:
	print("Min distance:")
	print(row)


requete = " SELECT day,month,year, COUNT(*) FROM e28.trip_days GROUP BY day,month,year;"
rows = session.execute(requete)
lines = []
for row in rows:
	line = (row.day, row.month, row.year, row.count)
	lines.append(line)

sorted_counts = sorted(lines, key=lambda tup: tup[3])
print(sorted_counts)

select hour, count(hour) from trip_days where day = 1 and month=1 and year=2014 group by hour;
 --> more during night


requete = "SELECT origin_call, count(*) FROM e28.trip_origin_calls GROUP BY origin_call;"
rows = session.execute(requete)
lines = []
for row in rows:
	line = (row.origin_call, row.count)
	lines.append(line)

sorted_counts = sorted(lines, key=lambda tup: tup[1])
print(sorted_counts)
print(np.mean([x[1] for x in sorted_counts]))


requete = "SELECT taxi_id, count(*) FROM e28.trip_taxis GROUP BY taxi_id;"
rows = session.execute(requete)
lines = []
for row in rows:
	line = (row.taxi_id, row.count)
	lines.append(line)

sorted_counts = sorted(lines, key=lambda tup: tup[1])
print(sorted_counts)
print(np.mean([x[1] for x in sorted_counts]))



requete = "select trip_id, max(duration) from trip_duration ;"
rows = session.execute(requete)

for row in rows:
	line = (row[0], row[1])

print(line)
