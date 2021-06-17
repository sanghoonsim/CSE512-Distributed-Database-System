from tester1 import DATABASE_NAME
import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    createDB(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    cur.execute("CREATE TABLE " + ratingstablename + " (userid INTEGER not null, t1 CHAR, movieid INTEGER, t2 CHAR, rating FLOAT, t3 CHAR, timestamp BIGINT);")
    file = open(ratingsfilepath, 'r')
    cur.copy_from(file, ratingstablename, ':')
    cur.execute("ALTER TABLE " + ratingstablename + " DROP column t1, DROP column t2, DROP column t3, DROP column timestamp;")
    con.commit()
    cur.close()
    

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    interval = 5 / numberofpartitions
    start = 0
    sink = start + interval
    partition_num = 0
    for i in range(0, numberofpartitions):
        if partition_num == 0:  
            cur.execute("DROP TABLE IF EXISTS range_ratings_part" + str(partition_num) +";")
            cur.execute("CREATE TABLE range_ratings_part" + str(partition_num) + " (userid INTEGER, movieid INTEGER, rating FLOAT);")
            cur.execute("INSERT INTO range_ratings_part" + str(partition_num) + 
            " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + 
            ratingstablename + " WHERE rating >=" + str(start) + " AND rating <=" + str(sink) + ";")
            partition_num += 1
            start = start + interval
            sink = start + interval
            con.commit()
        else:
            cur.execute("DROP TABLE IF EXISTS range_ratings_part" + str(partition_num) +";")
            cur.execute("CREATE TABLE range_ratings_part" + str(partition_num) + " (userid INTEGER, movieid INTEGER, rating FLOAT);")
            cur.execute("INSERT INTO range_ratings_part" + str(partition_num) + 
            " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + 
            ratingstablename + " WHERE rating >" + str(start) + " AND rating <=" + str(sink) + ";")
            partition_num += 1
            start = start + interval
            sink = start + interval
            con.commit()            
    cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    for i in range(0, numberofpartitions):
        cur.execute("DROP TABLE IF EXISTS round_robin_ratings_part" + str(i) + ";")
        cur.execute("CREATE TABLE round_robin_ratings_part" + str(i) + " (userid INTEGER, movieid INTEGER, rating FLOAT);")
        cur.execute("INSERT INTO round_robin_ratings_part" + str(i) + " (userid, movieid, rating) SELECT userid, movieid, rating" +
        " FROM (SELECT userid, movieid, rating, row_number() over() as rn"
        " FROM " + ratingstablename + ") as temp WHERE (temp.rn - 1) % " + str(numberofpartitions) + "=" + str(i) + ";")
        con.commit()
    cur.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):    
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM " + ratingstablename + ";")
    row = cur.fetchone()[0]
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name like'%round_robin%'; ")
    round_table = cur.fetchall()
    partition_count = len(round_table)
    row_num = []
    for r in range(0, partition_count):
        cur.execute("SELECT COUNT(*) FROM " + str(round_table[r][0]) + ";")
        row_num.append(cur.fetchone()[0])
    start = row_num[0]
    for r in range(1, partition_count):
        if row_num[r] < start:
            cur.execute("INSERT INTO " + round_table[r][0] + " (userid, movieid, rating) VALUES (" +
            str(userid)+ ", " + str(itemid) + ", " + str(rating) + ");")
            break
    if r == partition_count-1:
        cur.execute("INSERT INTO " + round_table[0][0] + " (userid, movieid, rating) VALUES (" +
        str(userid) + ", " + str(itemid)+ ", " + str(rating) + ");")          
    cur.execute("INSERT INTO "+ ratingstablename + "(userid, movieid, rating) VALUES (" +
    str(userid)+ ", " + str(itemid) + ", " + str(rating) + ");")    
    con.commit()
    cur.close()



def rangeInsert(ratingstablename, userid, itemid, rating, openconnection): 

    con = openconnection
    cur = con.cursor()
    cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating) VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")
    cur.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME LIKE '%range_ratings_part%';")
    t_name = cur.fetchall()
    start = 0
    interval = float(5) / len(t_name)
    sink = start + interval
    partition_num = 0
    for i in t_name:
        if partition_num == 0:
            if rating >= start and rating <= sink:
                cur.execute("INSERT INTO range_ratings_part" + str(partition_num) + " (userid, movieid, rating) VALUES (" + 
                str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")
                break
        else:
            if rating > start and rating <= sink:
                cur.execute("INSERT INTO range_ratings_part" + str(partition_num) + " (userid, movieid, rating) VALUES (" + 
                str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")
                break
        start = float(start+interval)
        sink = float(start+interval)
        partition_num += 1
    con.commit()
    cur.close()




def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    range_list = []
    round_list = []
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME LIKE '%range_ratings%';")
    for row in cur.fetchall():
        cur.execute("SELECT * FROM " + row[0] + " WHERE rating >= " + str(ratingMinValue) + " AND rating <=" + str(ratingMaxValue) + ";")
        for ro in cur.fetchall():
            temp = row + ro
            temp = tuple(temp)
            range_list.append(temp)
    cur.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME LIKE '%round_robin%';")
    for row in cur.fetchall():
        cur.execute("SELECT * FROM " + row[0] + " WHERE rating >= " + str(ratingMinValue) + " AND rating <=" + str(ratingMaxValue) + ";")
        for ro in cur.fetchall():
            temp = row + ro
            temp = tuple(temp)
            round_list.append(temp)
    with open(outputPath, 'w') as file:
        for row in range_list:
            file.write(','.join(str(r) for r in row))
            file.write('\n')
        for row in round_list:
            file.write(','.join(str(r) for r in row))
            file.write('\n')            
    con.commit()
    cur.close()         

def pointQuery(ratingValue, openconnection, outputPath):
    range_list = []
    round_list = []
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME LIKE '%range_ratings%';")
    for row in cur.fetchall():
        cur.execute("SELECT * FROM " + row[0] + " WHERE rating = " + str(ratingValue) + ";")
        for ro in cur.fetchall():
            temp = row + ro
            temp = tuple(temp)
            range_list.append(temp)
    cur.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME LIKE '%round_robin%';")
    for row in cur.fetchall():
        cur.execute("SELECT * FROM " + row[0] + " WHERE rating = " + str(ratingValue) + ";")
        for ro in cur.fetchall():
            temp = row + ro
            temp = tuple(temp)
            round_list.append(temp)
    with open(outputPath, 'w') as file:
        for row in range_list:
            file.write(','.join(str(r) for r in row))
            file.write('\n')
        for row in round_list:
            file.write(','.join(str(r) for r in row))
            file.write('\n')            
    con.commit()
    cur.close() 


def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
