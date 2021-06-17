#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading
THREAD_NO = 5
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    con = openconnection
    cur = con.cursor()
    threads = [0] * THREAD_NO
    cur.execute("SELECT MAX(" + SortingColumnName + "), MIN(" + SortingColumnName + ") FROM " + InputTable + ";")
    max_col, min_col = cur.fetchone()
    interval = float(max_col - min_col) / THREAD_NO
    temp = "temp_tbl"
    for i in range(THREAD_NO):
        min_range = min_col + (i*interval)
        max_range = min_range + interval
        threads[i] = threading.Thread(target=SortTable, args=(InputTable, min_range, max_range, temp, SortingColumnName, i, openconnection))
        threads[i].start()
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable + "';")
    schema = cur.fetchall()
    cur.execute("DROP TABLE IF EXISTS " + OutputTable + ";")
    cur.execute("CREATE TABLE " + OutputTable + " (TEMP INTEGER);")
    for row in range(len(schema)):
        cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema[row][0] + " " + schema[row][1] + ";")
    cur.execute("ALTER TABLE " + OutputTable + " DROP COLUMN TEMP")
    for i in range(THREAD_NO):
        threads[i].join()
        temp_t = temp + str(i)
        cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + temp_t + ";")
    cur.close()
    con.commit()

def SortTable (InputTable, min_range, max_range, temp_tbl, SortingColumnName, i, openconnection):
    con = openconnection
    cur = con.cursor()
    temp = temp_tbl + str(i)
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable + "';")
    schema = cur.fetchall()
    cur.execute("DROP TABLE IF EXISTS " + temp + ";")
    cur.execute("CREATE TABLE " + temp + " (TEMP INTEGER);")
    for row in range(len(schema)):
        cur.execute("ALTER TABLE " + temp + " ADD COLUMN " + schema[row][0] + " " + schema[row][1] + ";")
    cur.execute("ALTER TABLE " + temp + " DROP COLUMN TEMP")
    if i == 0:
        cur.execute("INSERT INTO " + temp + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + 
        " >= " + str(min_range) + " AND " + SortingColumnName + " <= " + str(max_range) + " ORDER BY " + SortingColumnName + " ASC;")
    else:
        cur.execute("INSERT INTO " + temp + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName +
        " > " + str(min_range) + " AND " + SortingColumnName + " <= " + str(max_range) + " ORDER BY " + SortingColumnName + " ASC;")

def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    con = openconnection
    cur = con.cursor()
    threads = [0] * THREAD_NO
    cur.execute("SELECT MAX(" + Table1JoinColumn + "), MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + ";")
    max_col_1, min_col_1 = cur.fetchone()
    cur.execute("SELECT MAX(" + Table2JoinColumn + "), MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + ";")
    max_col_2, min_col_2 = cur.fetchone()
    max_col = max(max_col_1, max_col_2)
    min_col = min(min_col_1, min_col_2)
    interval = float(max_col - min_col) / THREAD_NO
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "';")
    schema1 = cur.fetchall()
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "';")
    schema2 = cur.fetchall()
    temp1_ = "temp1_"
    temp2_ = "temp2_"
    temp_output = "temp_output_"
    for i in range(THREAD_NO):
        min_range = min_col + (i*interval)
        max_range = min_range + interval
        threads[i] = threading.Thread(target=JoinTable, args=(InputTable1, InputTable2, schema1, schema2, min_range, max_range,
        temp1_, temp2_, temp_output, Table1JoinColumn, Table2JoinColumn, i, openconnection))
        threads[i].start()
    cur.execute("DROP TABLE IF EXISTS " + OutputTable + ";")
    cur.execute("CREATE TABLE " + OutputTable + " (TEMP INTEGER);")
    for row in range(len(schema1)):
        cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema1[row][0] + " " + schema1[row][1] + ";")
    cur.execute("ALTER TABLE " + OutputTable + " DROP COLUMN TEMP")
    for row in range(len(schema2)):
        cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema2[row][0] + " " + schema2[row][1] + ";")
    for i in range(THREAD_NO):
        threads[i].join()
        semi_output = temp_output + str(i)
        cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + semi_output + ";")
    cur.close()
    con.commit()


def JoinTable(InputTable1, InputTable2, schema1, schema2, min_range, max_range, temp1_prefix, temp2_prefix, temp_output_prefix, Table1JoinColumn, Table2JoinColumn, i, openconnection):
    con = openconnection
    cur = con.cursor()
    temp1_ = temp1_prefix + str(i)
    temp2_ = temp2_prefix + str(i)
    temp_output = temp_output_prefix + str(i)
    cur.execute("DROP TABLE IF EXISTS " + temp1_)
    cur.execute("DROP TABLE IF EXISTS " + temp2_)
    cur.execute("DROP TABLE IF EXISTS " + temp_output)
    cur.execute("CREATE TABLE " + temp1_ + " (TEMP INTEGER);")
    cur.execute("CREATE TABLE " + temp2_ + " (TEMP INTEGER);")
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "';")
    schema1 = cur.fetchall()
    for row in range(len(schema1)):
        cur.execute("ALTER TABLE " + temp1_ + " ADD COLUMN " + schema1[row][0] + " " + schema1[row][1] + ";")
    cur.execute("ALTER TABLE " + temp1_ + " DROP COLUMN TEMP")
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "';")
    schema2 = cur.fetchall()
    for row in range(len(schema2)):
        cur.execute("ALTER TABLE " + temp2_ + " ADD COLUMN " + schema2[row][0] + " " + schema2[row][1] + ";")
    cur.execute("ALTER TABLE " + temp2_ + " DROP COLUMN TEMP")

    cur.execute("CREATE TABLE " + temp_output + " (TEMP INTEGER);")
    for row in range(len(schema1)):
        cur.execute("ALTER TABLE " + temp_output + " ADD COLUMN " + schema1[row][0] + " " + schema1[row][1] + ";")
    cur.execute("ALTER TABLE " + temp_output + " DROP COLUMN TEMP")
    for row in range(len(schema2)):
        cur.execute("ALTER TABLE " + temp_output + " ADD COLUMN " + schema2[row][0] + " " + schema2[row][1] + ";")
    if i == 0:
        cur.execute("INSERT INTO " + temp1_ + " SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + ">=" + str(min_range) + 
        " AND " + Table1JoinColumn + "<=" + str(max_range))
        cur.execute("INSERT INTO " + temp2_ + " SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + ">=" + str(min_range) + 
        " AND " + Table2JoinColumn + "<=" + str(max_range))
    else:
        cur.execute("INSERT INTO " + temp1_ + " SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + ">" + str(min_range) + 
        " AND " + Table1JoinColumn + "<=" + str(max_range))
        cur.execute("INSERT INTO " + temp2_ + " SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + ">" + str(min_range) + 
        " AND " + Table2JoinColumn + "<=" + str(max_range))        
    cur.execute("INSERT INTO " + temp_output + " SELECT * FROM " + temp1_ + " A INNER JOIN " + temp2_ + " B ON A." + Table1JoinColumn + " = B." + Table2JoinColumn + ";")


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
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
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


