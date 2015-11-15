#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):

    try:
        cur = openconnection.cursor()

        cur.execute("DROP TABLE IF EXISTS Ratings")
        cur.execute("CREATE TABLE " + ratingstablename + " (UserID integer,tempdelimiter text, MovieID integer,tempdelimiter2 text,Rating numeric(2,1),tempdelimiter3 text, Timestamp double precision)")

        insertquery = "COPY " + ratingstablename + "(UserID,tempdelimiter, MovieID,tempdelimiter2, Rating,tempdelimiter3, Timestamp) FROM '" + ratingsfilepath + "' WITH DELIMITER ':'"
        cur.execute(insertquery)
        
        cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN tempdelimiter, DROP COLUMN tempdelimiter2, DROP COLUMN tempdelimiter3, DROP COLUMN Timestamp")
        
        openconnection.commit()
        cur.close()

    except psycopg2.DatabaseError, e:
    
        if openconnection:
            openconnection.rollback()
    
        print 'DatabaseError, %s' % e   
    



def rangepartition(ratingstablename, numberofpartitions, openconnection):
    
    try:
        cur = openconnection.cursor()
        #" + ratingstablename + "
        cur.execute("DROP TABLE IF EXISTS RANGEPARTITION_INFO")
        cur.execute("CREATE TABLE RANGEPARTITION_INFO (rangetablename text, minrange double precision, maxrange double precision)")
        
        diff = 5.0 / numberofpartitions
        minrange = 0.0
        maxrange = diff

        for partition in range(0,numberofpartitions):
            cur.execute("DROP TABLE IF EXISTS RANGE_PART" + str(partition+1))
            cur.execute("CREATE TABLE RANGE_PART" + str(partition+1) + " (LIKE " + ratingstablename +")")
            cur.execute("INSERT INTO RANGEPARTITION_INFO VALUES('RANGE_PART" + str(partition+1) + "'," + str(minrange)+"," + str(maxrange)+")")
            cur.execute("INSERT INTO RANGE_PART" + str(partition+1)+" SELECT * FROM " + ratingstablename + " WHERE Rating>=" + str(minrange) + " AND Rating<" + str(maxrange))
            minrange = minrange + diff
            maxrange = maxrange + diff
            if maxrange == 5.0:
                maxrange = 5.1

        openconnection.commit()    
        cur.close()

    except psycopg2.DatabaseError, e:

        if openconnection:
            openconnection.rollback()

        print 'Database Error, %s' % e


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    
    try:
        cur = openconnection.cursor()

        cur.execute("DROP TABLE IF EXISTS ROUNDROBINPARTITION_INFO")
        cur.execute("CREATE TABLE ROUNDROBINPARTITION_INFO (roundrobintablename text, roundcounter integer, numberofpartitions integer)")
        
        for partition in range(0,numberofpartitions):
            cur.execute("DROP TABLE IF EXISTS RROBIN_PART" + str(partition))
            cur.execute("CREATE TABLE RROBIN_PART" + str(partition) + " (LIKE " + ratingstablename +")")
            #t_name = numberofpartitions 
            cur.execute("INSERT INTO RROBIN_PART" + str(partition)+" SELECT UserID,MovieID,Rating FROM (SELECT UserID,MovieID,Rating,ROW_NUMBER() OVER() AS ROWNO FROM Ratings) Ratings where Ratings.ROWNO%" + str(numberofpartitions) + "=" + str(partition))
        
        cur.execute("SELECT COUNT(*) FROM " + ratingstablename)
        ans = cur.fetchall()
        count = ans[0][0]
        roundcounter = count % numberofpartitions
        roundcounter = roundcounter + 1
        if roundcounter == numberofpartitions:
            roundcounter = 0
        cur.execute("INSERT INTO ROUNDROBINPARTITION_INFO VALUES('" + ratingstablename + "'," + str(roundcounter) + "," + str(numberofpartitions) + ")")

        openconnection.commit()    
        cur.close()

    except psycopg2.DatabaseError, e:

        if openconnection:
            openconnection.rollback()

        print 'Database Error, %s' % e


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    
    try:
        cur = openconnection.cursor()

        cur.execute("SELECT roundcounter, numberofpartitions FROM ROUNDROBINPARTITION_INFO WHERE roundrobintablename='" + ratingstablename + "'")

        result = cur.fetchall()
        counter = result[0][0]
        nopartitions = result[0][1]

        cur.execute("INSERT INTO RROBIN_PART" + str(counter) + " VALUES(" + str(userid) + "," + str(itemid) + "," + str(rating) + ")")
        
        counter = counter + 1
        if counter == nopartitions:
            counter = 0

        cur.execute("UPDATE ROUNDROBINPARTITION_INFO SET ROUNDCOUNTER=" + str(counter) + " WHERE ROUNDROBINTABLENAME='" + ratingstablename + "'")

        openconnection.commit()    
        cur.close()

    except psycopg2.DatabaseError, e:

        if openconnection:
            openconnection.rollback()

        print 'Database Error, %s' % e


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    try:
        cur = openconnection.cursor()

        cur.execute("SELECT rangetablename FROM RANGEPARTITION_INFO WHERE " + str(rating) +">minrange AND " + str(rating) + "<=maxrange")

        rows = cur.fetchall()
        row = rows[0]
        tablename = row[0]

        cur.execute("INSERT INTO " + tablename + " VALUES(" + str(userid) + "," + str(itemid) + "," + str(rating) + ")")

        openconnection.commit()    
        cur.close()

    except psycopg2.DatabaseError, e:

        if openconnection:
            openconnection.rollback()

        print 'Database Error, %s' % e


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()


def deletepartitionsandexit(openconnection):

    try:
        cur = openconnection.cursor()

        cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='public' AND TABLE_TYPE='BASE TABLE'")
        tables = cur.fetchall()
        for table in tables:
           cur.execute("DROP TABLE IF EXISTS " + table[0] + " ")

        openconnection.commit()
        cur.close()

    except psycopg2.DatabaseError, e:

        if openconnection:
            openconnection.rollback()

        print 'Database Error, %s' % e

# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
