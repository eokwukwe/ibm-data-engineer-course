import psycopg2
import mysql.connector
from dotenv import dotenv_values

config = dotenv_values('../.env')

mysql_conn = mysql.connector.connect(
    user=config["MYSQL_DATABASE_USER"],
    password=config["MYSQL_DATABASE_PASSWORD"],
    host=config["MYSQL_DATABASE_HOST"],
    database=config["MYSQL_DATABASE_NAME"],
    port=config["MYSQL_DATABASE_PORT"]
)

pg_conn = psycopg2.connect(
    user=config["PG_DATABASE_USER"],
    password=config["PG_DATABASE_PASSWORD"],
    host=config["PG_DATABASE_HOST"],
    database=config["PG_DATABASE_NAME"],
    port=config["PG_DATABASE_PORT"]
)


def get_last_rowid():
    '''
    Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
    The function must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

    Returns:
        int: the last rowid of the sales_data table.
    '''
    with pg_conn.cursor() as cursor:
        cursor.execute(
            "SELECT rowid FROM sales_data ORDER BY rowid DESC LIMIT 1")
        row = cursor.fetchone()

    return row[0]


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)


def get_latest_records(rowid):
    '''
    List out all records in MySQL database with rowid greater than the one on the Data warehouse

    The function must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

    Args:
        rowid (int): the last rowid of the sales_data table.

    Returns:
        list: a list of all records that have a rowid greater than the rowid
        passed.
    '''
    with mysql_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM sales_data WHERE rowid > %s", (rowid, ))
        rows = cursor.fetchall()

    return rows


new_records = get_latest_records(last_row_id)
print("New rows on staging datawarehouse = ", len(new_records))


def insert_records(records):
    '''
    Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.

    The function must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

    Args:
        records (list): a list of records to be inserted into the sales_data table.

    Returns:
        None
    '''
    with pg_conn.cursor() as cursor:
        sql = """INSERT INTO sales_data (
            rowid, product_id, customer_id, quantity
        ) VALUES(%s, %s, %s, %s)
        """

        cursor.executemany(sql, records)

        pg_conn.commit()


insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
mysql_conn.close()
# disconnect from DB2 or PostgreSql data warehouse
pg_conn.close()

# End of program
