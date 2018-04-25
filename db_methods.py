import time
import psycopg2
from psycopg2 import sql
from private_settings import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER


def connect_to_db():
    try:
        connection = psycopg2.connect(
            "dbname={dbname} user={user} host={host} password={password}".format(
                dbname=DB_NAME, user=DB_USER, host=DB_HOST, password=DB_PASSWORD
            ))
        connection.autocommit = True
        cursor = connection.cursor()
        # remember to close connection and cursor after executing a query
        return connection, cursor
    except Exception as e:
        print('ERROR: Unable to connect to db --> ', e)


def drop_table(cursor, table):
    try:
        cursor.execute(
            sql.SQL('DROP TABLE IF EXISTS {};')
            .format(sql.Identifier(table)))
    except Exception as e:
        print('ERROR: Unable to drop table --> ', e)


def create_table(cursor, table):
    try:
        cursor.execute(
            sql.SQL('''CREATE TABLE IF NOT EXISTS {table} (
                movieId varchar(255) NOT NULL,
                title varchar(255) NOT NULL,
                genres varchar(255)
            );
                ''').format(table=sql.Identifier(table)))
    except Exception as e:
        print('ERROR: Unable to create table --> ', e)


def query_sql(cursor, query, params):
    start = time.time()

    try:
        cursor.execute(query, params)
    except Exception as e:
        print('ERROR: Unable to execute query --> ', e)

    print('Finished {query} in {time} seconds'.format(
        time=time.time() - start, query=query))


def insert_row(table, cursor, params):
    query = sql.SQL('INSERT INTO {} VALUES (%s, %s, %s);').format(
        sql.Identifier(table))
    query_sql(cursor=cursor, params=params, query=query)


if __name__ == '__main__':
    connection, cursor = connect_to_db()
    # create_table(cursor=cursor, table='movie_dataset')
    cursor.close()
    connection.close()
