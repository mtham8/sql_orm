import psycopg2
from private_settings import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER


def connect_to_db():
    try:
        conn = psycopg2.connect(
            "dbname={dbname} user={user} host={host} password={password}".format(dbname=DB_NAME, user=DB_USER, host=DB_HOST, password=DB_PASSWORD))
        conn.autocommit = True
    except:
        print "I am unable to connect to the database"
    cur = conn.cursor()
    try:
        cur.execute("""CREATE TABLE test_customer_info (
            ID INT PRIMARY KEY     NOT NULL,
            NAME           TEXT    NOT NULL,
            AGE            INT     NOT NULL,
            ADDRESS        CHAR(50)
        );""")
    except:
        print "I can't test the database!"


if __name__ == '__main__':
    connect_to_db()
