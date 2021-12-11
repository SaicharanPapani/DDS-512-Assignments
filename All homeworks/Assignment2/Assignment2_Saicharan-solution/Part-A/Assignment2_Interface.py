#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 21:48:35 2021

@author: saicharanpapani
"""

#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading


# Do not close the connection inside this file i.e. do not perform openConnection.close()

def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    #Implement ParallelJoin Here.
    # Remove this once you are done with implementation
    con = openConnection
    cur = con.cursor()
    num_of_threads = 5
    # dropping point partition tables, if exists
    for i in range(1, num_of_threads):
        query_str = "DROP TABLE IF EXISTS pts"+str(i)+";"
        cur.execute(query_str)
    # dropping point partition tables, if exists
    for i in range(1, num_of_threads):
        query_str = "DROP TABLE IF EXISTS rts"+str(i)+";"
        cur.execute(query_str)
    # find the largest latitude
    cur.execute('select * from rectangles order by latitude2 desc') 
    greatest_Latitude = cur.fetchone()[3]
    
    # find the smallest latitude
    cur.execute('select * from rectangles order by latitude1 asc') 
    smallest_latitude = cur.fetchone()[1]
    
    # find the largest longitude
    cur.execute('select * from rectangles order by longitude2 desc') 
    greatest_Longitude = cur.fetchone()[2]
    
    # find the smallest longitude
    cur.execute('select * from rectangles order by longitude1 asc') 
    smallest_longitude = cur.fetchone()[0]
    
    # finding the center point, which becomes our origin and we make 4 fragments.
    center_lati = (greatest_Latitude - smallest_latitude) / 2   
    center_longi = (greatest_Longitude - smallest_longitude) / 2   
    
    # creating points fragments
    query_str = "CREATE TABLE pts1 AS SELECT * FROM points WHERE latitude < "+str(center_lati)+" and longitude < "+str(center_longi)+";"
    cur.execute(query_str)
    query_str = "CREATE TABLE pts2 AS SELECT * FROM points WHERE latitude < "+str(center_lati)+" and longitude > "+str(center_longi)+";"
    cur.execute(query_str)
    query_str = "CREATE TABLE pts3 AS SELECT * FROM points WHERE latitude > "+str(center_lati)+" and longitude < "+str(center_longi)+";"
    cur.execute(query_str)
    query_str = "CREATE TABLE pts4 AS SELECT * FROM points WHERE latitude > "+str(center_lati)+" and longitude > "+str(center_longi)+";"
    cur.execute(query_str)
    # creating rectangles fragments
    query_str = "CREATE TABLE rts1 AS SELECT * FROM rectangles WHERE latitude1 < "+str(center_lati)+" and longitude1 < "+str(center_longi)+";"
    cur.execute(query_str)
    query_str = "CREATE TABLE rts2 AS SELECT * FROM rectangles WHERE latitude1 < "+str(center_lati)+" and longitude1 > "+str(center_longi)+";"
    cur.execute(query_str)
    query_str = "CREATE TABLE rts3 AS SELECT * FROM rectangles WHERE latitude1 > "+str(center_lati)+" and longitude1 < "+str(center_longi)+";"
    cur.execute(query_str)
    query_str = "CREATE TABLE rts4 AS SELECT * FROM rectangles WHERE latitude1 > "+str(center_lati)+" and longitude1 > "+str(center_longi)+";"
    cur.execute(query_str)
    print("completed fragments creation")
    query_str = "select * from pts1";
    cur.execute(query_str)
    db_version = cur.fetchone()
    # print(db_version)
    threads = []
    for i in range(1, num_of_threads):
        query_str = "DROP TABLE IF EXISTS JoinFragment"+str(i)+";"
        cur.execute(query_str)
    for i in range(1, num_of_threads):
        spatial_join = "CREATE TABLE JoinFragment"+str(i)+" AS Select count(ps.geom) as points_count, rs.geom from rts"+str(i)+" as rs JOIN pts"+str(i)+" as ps on ST_Contains(rs.geom, ps.geom) group by rs.geom order by points_count"
        t = threading.Thread(target=cur.execute, args=(spatial_join,))
        threads.append(t)
        print("Starting Spatial Join in thread: "+str(i))
        threads[-1].start()
    query_str = "DROP TABLE IF EXISTS "+outputTable+";"
    cur.execute(query_str)
    query_str = "CREATE TABLE "+outputTable+" AS (" + " UNION ".join(["select * from JoinFragment"+str(i) for i in range(1,num_of_threads)]) + ") order by points_count;"
    cur.execute(query_str)
    cur.execute("Select points_count from "+outputTable+";")
    rows = cur.fetchall()
    #print(rows)
    with open(outputPath, "w") as file:
        for row in rows:
            file.write(str(row[0])+"\n")
    file.close()
    for i in range(num_of_threads-1):
        threads[i].join()
    
    cur.close()
    con.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


