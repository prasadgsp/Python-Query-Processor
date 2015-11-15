#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    try:
    	cur = openconnection.cursor()

    	f = open('RangeQueryOut.txt', 'w')

    	query = "SELECT * FROM information_schema.tables WHERE TABLE_NAME=\'range" + ratingsTableName + "metadata\'";
    	cur.execute(query)
    	ans = cur.fetchone()

    	if ans:
    		print '\tRange Query performed on Range Partitioned Tables'
    		cur.execute("SELECT partitionnum FROM RANGE" + ratingsTableName + "METADATA WHERE (MINRATING<" + str(ratingMinValue) + " AND MAXRATING>=" + str(ratingMinValue) + ") OR ( MINRATING<" + str(ratingMaxValue) + " AND MAXRATING>=" + str(ratingMaxValue) + ") OR ( MINRATING >= " + str(ratingMinValue) + " AND MAXRATING <= " + str(ratingMaxValue) + ")")
    		rows = cur.fetchall()
    		for i in rows:
    			query = "SELECT * FROM RANGE" + ratingsTableName + "PART" + str(i[0]) + " WHERE RATING>=" + str(ratingMinValue) + " AND RATING <=" + str(ratingMaxValue);
    			cur.execute(query)
    			rows = cur.fetchall()
    			for row in rows:
    				f.write("range" + ratingsTableName + "part" + str(i[0]) + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "\n")
    	else:
    		print '\tRange Partitioning Not Done to perform Range Query on it'


    	query = "SELECT * FROM information_schema.tables WHERE TABLE_NAME=\'roundrobin" + ratingsTableName + "metadata\'";
    	cur.execute(query)
    	ans = cur.fetchone()

    	if ans:
    		print '\tRange Query performed on Round Robin Partitioned Tables'
    		cur.execute("SELECT * FROM ROUNDROBIN" + ratingsTableName + "METADATA")
    		rows = cur.fetchall()
    		numpartitions = rows[0][0]
    		for i in range(0, numpartitions):
    			cur.execute("SELECT * FROM ROUNDROBIN" + ratingsTableName + "PART" + str(i) + " WHERE RATING>=" + str(ratingMinValue) + " AND RATING <=" + str(ratingMaxValue))
    			rows = cur.fetchall()
    			for row in rows:
    				f.write("roundrobin" + ratingsTableName + "part" + str(i) + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "\n")
    	else:
    		print '\tRound Robin Partitioning Not Done to perform Range Query on it'

    	f.close()

    	cur.close()

    except Exception, e:
        if openconnection:
            openconnection.rollback()

        f.close()
    
        print 'DatabaseError, %s' % e 


def PointQuery(ratingsTableName, ratingValue, openconnection):
    try:
    	cur = openconnection.cursor()

    	f = open('PointQueryOut.txt', 'w')
    	
    	query = "SELECT * FROM information_schema.tables WHERE TABLE_NAME=\'range" + ratingsTableName + "metadata\'";
    	cur.execute(query)
    	ans = cur.fetchone()

    	if ans:
    		print '\tPoint Query performed on Range Partitioned Tables'
    		cur.execute("SELECT partitionnum FROM RANGE" + ratingsTableName + "METADATA WHERE MINRATING<" + str(ratingValue) + " AND MAXRATING>=" + str(ratingValue) )
    		rows = cur.fetchall()
    		for i in rows:
    			query = "SELECT * FROM RANGE" + ratingsTableName + "PART" + str(i[0]) + " WHERE RATING = " + str(ratingValue);
    			cur.execute(query)
    			rows = cur.fetchall()
    			for row in rows:
    				f.write("range" + ratingsTableName + "part" + str(i[0]) + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "\n")
    	else:
    		print '\tRange Partitioning Not Done to perform Point Query on it'			



    	query = "SELECT * FROM information_schema.tables WHERE TABLE_NAME=\'roundrobin" + ratingsTableName + "metadata\'";
    	cur.execute(query)
    	ans = cur.fetchone()

    	if ans:
    		print '\tPoint Query performed on Round Robin Partitioned Tables'
    		cur.execute("SELECT * FROM ROUNDROBIN" + ratingsTableName + "METADATA")
    		rows = cur.fetchall()
    		numpartitions = rows[0][0]
    		for i in range(0, numpartitions):
    			cur.execute("SELECT * FROM ROUNDROBIN" + ratingsTableName + "PART" + str(i) + " WHERE RATING=" + str(ratingValue))
    			rows = cur.fetchall()
    			for row in rows:
    				f.write("roundrobin" + ratingsTableName + "part" + str(i) + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "\n")
    	else:
    		print '\tRound Robin Partitioning Not Done to perform Point Query on it'

    	f.close()

    	cur.close()

    except Exception, e:
        if openconnection:
            openconnection.rollback()

        f.close()
    
        print 'DatabaseError, %s' % e 
