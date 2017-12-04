# -*- coding: utf-8 -*-
"""
Created on Fri May 19 10:58:28 2017

@author: nf26p006
"""

import csv
import datetime
import pandas as pd

file = pd.read_csv("/home/e28/train.csv", sep=";", encoding = 'latin_1')

from cassandra.cluster import Cluster

cluster = Cluster()

session = cluster.connect('e28')
session.default_timeout = 9999

def dist(lon1, lon2, lat1, lat2):
	import numpy as np
	RT = 6371008
	d = np.sqrt(
	   ((lon1-lon2)*np.cos((lat1+lat2)/2/180*np.pi))**2
	)/180*np.pi*RT
	return d

def insert(table_name):
	with open('/train.csv') as f:
		nbligne = 0
		l=f.readline()
		while True:
			l=f.readline()
			nbligne += 1
			if len(l)==0:
				break

			data = l.split("\",\"")
			id_trip = data[0][1:]
			call_type = data[1]
			origin_call = data[2]
			origin_stand = data[3]
			id_taxi = data[4]
			timestamp = data[5]
			day_type = data[6]
			data_missing = data[7]
			chemin = data[8][2:-4]
			positions = chemin.split("],[")


			date = datetime.datetime.fromtimestamp(int(timestamp))
			year = date.year
			month = date.month
			day = date.day
			hour = date.hour
			minute = date.minute
			duration=0


			Bdays = ['112013','2932013','3132013','2542013','152013','1062013','1582013','8122013','25122013','112014','1842014','2042014','2542014','152014','1062014','1582014','8122014','25122014']
			Cdays = ['31122012','2832013','3032013','2442013','3042013','962013','1482013','7122013','24122013','31122013','1742014','1942014','2442014','3042014','962014','1482014','7122014','24122014']

			daymonthyear = str(day) + str(month) + str(year)

			if daymonthyear in Bdays:
					day_type='B'

			if daymonthyear in Cdays:
					day_type='C'


			d_requete = "INSERT INTO e28." + table_name + " (trip_id, taxi_id"
			f_requete = "VALUES (%s, %s" % (id_trip, id_taxi)

			d_requete += ", year, month, day, hour, min, daytype"
			f_requete += ", %s, %s, %s, %s, %s, '%s'" % (year, month, day, hour, minute, day_type)

			d_requete += ", call_type"
			f_requete += ", '%s'" % (call_type)

			if(call_type == "A"):
				if call_type:
					d_requete += ", origin_call"
					f_requete += ", %s" %(origin_call)
				else:
					continue

			elif(call_type == "B"):
				if origin_stand:
					d_requete += ", origin_stand"
					f_requete += ", %s" %(origin_stand)
				else:
					continue

			if (len(positions)>=2):
				duration = len(positions) * 15
				duration = duration - 15
				lon1= float(positions[0].split(",")[0])
				lat1= float(positions[0].split(",")[1])
				lon2= float(positions[-1].split(",")[0])
				lat2= float(positions[-1].split(",")[1])
				distance = dist(lon1,lat1,lon2,lat2)
				lon1= "%.2f" % float(positions[0].split(",")[0])
				lat1= "%.2f" % float(positions[0].split(",")[1])
				lon2= "%.2f" % float(positions[-1].split(",")[0])
				lat2= "%.2f" % float(positions[-1].split(",")[1])

				d_requete+= ", starting_point, ending_point, dist, duration"
				f_requete+= ", '[%s,%s]','[%s,%s]', %s, %s" % (lon1,lat1,lon2,lat2,distance,duration)
			else:
				continue

			d_requete += ") "
			f_requete += ");"


			session.execute(d_requete+f_requete)
			print(nbligne)

table_names = ["trip_day_types"]
'''
insert("trip_departures")
'''
for table_name in table_names:
	insert(table_name)



# Grid generation
'''
for i in range(2500):
    if i%50==0 and i!=0:
        print ('')
    print ("(%.2f, %.2f):%4s "%((i%50)/100 - 8.25, i/(50*100) + 40.85 ,i))
'''


'''
CQLString = "insert into pollution_vehicules (id , lib_mrq , lib_mod_doss , lib_mod , dscom , cnit , tw , cod_cbr , hybride , puiss_admin_98 , puiss_max , typ_boite_nb_rapp , conso_urb , conso_exurb , conso_mixte , co2 , co_typ_1 , hc , nox , hcnox , pctl , masse_ordma_min , masse_ordma_max , champ_v9 , date_maj , Carrosserie , gamme) values (%d,'%s','%s','%s','%s','%s','%s','%s','%s',%d,%d,'%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d);"

for index, row in file.iterrows():
	session.execute(CQLString, (int(index), row['lib_mrq'], row['lib_mod_doss'], row['lib_mod'], row['dscom'], row['cnit'] , row['tw'] , row['cod_cbr'] , row['hybride'] , row['puiss_admin_98'] , row['puiss_max'] , row['typ_boite_nb_rapp'] , row['conso_urb'] , row['conso_exurb'] , row['conso_mixte'] , row['co2'] , row['co_typ_1'] , row['hc'] , row['nox'] , row['hcnox'] , row['pctl'] , row['masse_ordma_min'] , row['masse_ordma_max'] , row['champ_v9'] , row['date_maj'] , row['Carrosserie'] , row['gamme']) )
'''
