#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'movierating'
SECOND_TABLE_NAME = 'movieboxofficecollection'
SORT_COLUMN_NAME_FIRST_TABLE = 'movieid'
SORT_COLUMN_NAME_SECOND_TABLE = 'collection'
JOIN_COLUMN_NAME_FIRST_TABLE = 'movieid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'movieid'
##########################################################################################################



def RangePartition(InputTable, SortingColumnName, openconnection):

	cur = openconnection.cursor()
	
	cur.execute('DROP TABLE IF EXISTS ' + InputTable + 'RANGEPARTITION_INFO')
	cur.execute('CREATE TABLE ' + InputTable + 'RANGEPARTITION_INFO(TableNum integer PRIMARY KEY, minrange double precision, maxrange double precision)')

	cur.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable)
	minrange = cur.fetchall()[0][0]

	cur.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable)
	maxrange = cur.fetchall()[0][0]

	diff = (float)(maxrange - minrange) / 5

	for partition in range(0, 5):
		maxvalue = minrange + diff
		if(maxvalue==maxrange):
			maxvalue+=0.01;
		cur.execute("DROP TABLE IF EXISTS " + InputTable + "RANGE_PART" + str(partition+1))
		cur.execute('INSERT INTO ' + InputTable + 'RANGEPARTITION_INFO VALUES(' + str(partition+1) + ',' + str(minrange) + ',' + str(maxvalue) + ')') 
		cur.execute("CREATE TABLE " + InputTable + "RANGE_PART" + str(partition+1) + " (LIKE " + InputTable +")")
		cur.execute("INSERT INTO " + InputTable + "RANGE_PART" + str(partition+1)+" SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + ">=" + str(minrange) + " AND " + SortingColumnName + "<" + str(maxvalue))
		minrange = minrange + diff
	
	openconnection.commit()



def ParallelSortThread(InputTable, SortingColumnName, threadNum, listobj, openconnection):

	cur = openconnection.cursor()
	
	query = "SELECT * FROM " + InputTable + "RANGE_PART" + str(threadNum) + " ORDER BY " + SortingColumnName
	cur.execute(query)
	rows = cur.fetchall()
	listobj.append(rows)
	cur.execute("DROP TABLE IF EXISTS " + InputTable + "RANGE_PART" + str(threadNum))



def mergeParallelSort(OutputTable,openconnection,list1,list2,list3,list4,list5):

	cur = openconnection.cursor()

	for item in list1:
		if str(item)!="[]":
			values = str(item).lstrip('[').rstrip(']')
			query = "INSERT INTO " + OutputTable + " VALUES " + values
			cur.execute(query)

	for item in list2:
		if str(item)!="[]":
			values = str(item).lstrip('[').rstrip(']')
			query = "INSERT INTO " + OutputTable + " VALUES " + values
			cur.execute(query)

	for item in list3:
		if str(item)!="[]":
			values = str(item).lstrip('[').rstrip(']')
			query = "INSERT INTO " + OutputTable + " VALUES " + values
			cur.execute(query)

	for item in list4:
		if str(item)!="[]":
			values = str(item).lstrip('[').rstrip(']')
			query = "INSERT INTO " + OutputTable + " VALUES " + values
			cur.execute(query)

	for item in list5:
		if str(item)!="[]":
			values = str(item).lstrip('[').rstrip(']')
			query = "INSERT INTO " + OutputTable + " VALUES " + values
			cur.execute(query)

	openconnection.commit()



# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
	try:
		cur = openconnection.cursor()


    	# Range Partition table according to value of sorting column value so that each thread assigned a part

		RangePartition(InputTable, SortingColumnName, openconnection);

		# Perform Parallel Sorting

		cur.execute("DROP TABLE IF EXISTS " + OutputTable)
		cur.execute("CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable + " WHERE 1=2")

		list1 = []
		list2 = []
		list3 = []
		list4 = []
		list5 = []

		thread1 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'1',list1,openconnection))
		thread2 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'2',list2,openconnection))
		thread3 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'3',list3,openconnection))
		thread4 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'4',list4,openconnection))
		thread5 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'5',list5,openconnection))

		thread1.start()
		thread2.start()
		thread3.start()
		thread4.start()
		thread5.start()

		thread1.join()
		thread2.join()
		thread3.join()
		thread4.join()
		thread5.join()

		# print ("\n-----")
		# for item in list1:
		# 	print "!" + str(item) + "!"
		# 	if str(item)!="[]":
		# 		values = str(item).lstrip('[').rstrip(']')
		# 		print "!" + values + "!"
		# 		query = "INSERT INTO " + OutputTable + " VALUES " + values
		# 		cur.execute(query)
		# print ("\n-----")
		# for item in list2:
		# 	print "!" + str(item) + "!"
		# 	if str(item)!="[]":
		# 		values = str(item).lstrip('[').rstrip(']')
		# 		print "!" + values + "!"
		# 		query = "INSERT INTO " + OutputTable + " VALUES " + values
		# 		cur.execute(query)
		# print ("\n-----")
		# for item in list3:
		# 	print "!" + str(item) + "!"
		# 	if str(item)!="[]":
		# 		values = str(item).lstrip('[').rstrip(']')
		# 		print "!" + values + "!"
		# 		query = "INSERT INTO " + OutputTable + " VALUES " + values
		# 		cur.execute(query)
		# print ("\n-----")
		# for item in list4:
		# 	print "!" + str(item) + "!"
		# 	if str(item)!="[]":
		# 		values = str(item).lstrip('[').rstrip(']')
		# 		print "!" + values + "!"
		# 		query = "INSERT INTO " + OutputTable + " VALUES " + values
		# 		cur.execute(query)
		# print ("\n-----")
		# for item in list5:
		# 	print "!" + str(item) + "!"
		# 	if str(item)!="[]":
		# 		values = str(item).lstrip('[').rstrip(']')
		# 		print "!" + values + "!"
		# 		query = "INSERT INTO " + OutputTable + " VALUES " + values
		# 		cur.execute(query)		

		mergeParallelSort(OutputTable,openconnection,list1,list2,list3,list4,list5);

		cur.execute('DROP TABLE IF EXISTS ' + InputTable + 'RANGEPARTITION_INFO')

		con.commit()
		cur.close()

	except Exception, e:
		if openconnection:
			openconnection.rollback()
    
		print 'DatabaseError, %s' % e 



def ParallelJoinThread (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, threadNum, openconnection,outputTableValueSelect):
	
	cur = openconnection.cursor()
	tableName1 = InputTable1+ "RANGE_PART" + str(threadNum)
	tableName2 = InputTable2+ "RANGE_PART" + str(threadNum)

	index = outputTableValueSelect.find(Table1JoinColumn)
	finaltableselect = outputTableValueSelect[:index] + tableName1 + '.' + outputTableValueSelect[index:]

	query = "SELECT " + finaltableselect + " FROM " + tableName1 + "," + tableName2 + " WHERE " + tableName1 + "."  + Table1JoinColumn + "=" + tableName2 + "." + Table1JoinColumn

	cur.execute(query)
	rows = cur.fetchall()
	insertValues = "";
	for row in range(0,len(rows)):
		if (row != len(rows)-1):
			insertValues = insertValues + str(rows[row]) + ","
		else:
			insertValues = insertValues + str(rows[row])


	cur.execute("INSERT INTO " + OutputTable + " VALUES " + insertValues)
	cur.execute("DROP TABLE IF EXISTS " + InputTable1 + "RANGE_PART" + str(threadNum))
	cur.execute("DROP TABLE IF EXISTS " + InputTable2 + "RANGE_PART" + str(threadNum))


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

	try:
		cur = openconnection.cursor()


    	# Range Partition table according to value of sorting column value so that each thread assigned a part

		RangePartition(InputTable1, Table1JoinColumn, openconnection);
		RangePartition(InputTable2, Table2JoinColumn, openconnection);

		# # Perform Parallel Sorting

		cur.execute("DROP TABLE IF EXISTS " + OutputTable)
		
		cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "'")
		firsttablecolumns = cur.fetchall()

		outputTablecolumns = '('
		outputTableValueSelect = ''
		for rows in firsttablecolumns:
			outputTablecolumns = outputTablecolumns +  rows[0] + " " + rows[1] + ","
 			outputTableValueSelect = outputTableValueSelect + rows[0] + ","

		cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "'")
		secondtablecolumns = cur.fetchall()

		for rows in range(0,len(secondtablecolumns)):
			if (secondtablecolumns[rows][0] != Table1JoinColumn):
				if (rows == len(secondtablecolumns) - 1):
					outputTablecolumns = outputTablecolumns + secondtablecolumns[rows][0] + " " + secondtablecolumns[rows][1] + ")"
					outputTableValueSelect = outputTableValueSelect + secondtablecolumns[rows][0]
				else:
					outputTablecolumns = outputTablecolumns + secondtablecolumns[rows][0] + " " + secondtablecolumns[rows][1] + ","
					outputTableValueSelect = outputTableValueSelect + secondtablecolumns[rows][0] + ","

		cur.execute("CREATE TABLE " + OutputTable + " " + outputTablecolumns)

		thread1 = threading.Thread(target = ParallelJoinThread, args = (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,OutputTable,'1',openconnection,outputTableValueSelect))
		thread2 = threading.Thread(target = ParallelJoinThread, args = (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,OutputTable,'2',openconnection,outputTableValueSelect))
		thread3 = threading.Thread(target = ParallelJoinThread, args = (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,OutputTable,'3',openconnection,outputTableValueSelect))
		thread4 = threading.Thread(target = ParallelJoinThread, args = (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,OutputTable,'4',openconnection,outputTableValueSelect))
		thread5 = threading.Thread(target = ParallelJoinThread, args = (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,OutputTable,'5',openconnection,outputTableValueSelect))

		thread1.start()
		thread2.start()
		thread3.start()
		thread4.start()
		thread5.start()

		thread1.join()
		thread2.join()
		thread3.join()
		thread4.join()
		thread5.join()

		cur.execute('DROP TABLE IF EXISTS ' + InputTable1 + 'RANGEPARTITION_INFO')
		cur.execute('DROP TABLE IF EXISTS ' + InputTable2 + 'RANGEPARTITION_INFO')

		con.commit()
		cur.close()

	except Exception, e:
		if openconnection:
			openconnection.rollback()
    
		print 'DatabaseError, %s' % e 

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	#print "Performing Parallel Sort"
	#ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
	deleteTables('parallelJoinOutputTable', con);

	if con:
		con.close()

	except Exception as detail:
		print "Something bad has happened!!! This is the error ==> ", detail
