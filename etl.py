""" ETL """
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    """
    This function loads the staging tables to redshift.
    :param cur: cursor
    :param conn: connection
    :return:
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts data into the star schema.
    :param cur:
    :param conn:
    :return:
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function is the main function of the ETL process.
    :return:
    """
    # Read the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load staging tables to redshift
    print("Loading staging tables...")
    start_load_staging_tables = time.time()
    load_staging_tables(cur, conn)
    end_load_staging_tables = time.time()
    print("Loading staging tables took {} seconds".format(end_load_staging_tables - start_load_staging_tables))

    # Insert data into the star schema
    print("Inserting data into star schema...")
    start_insert_tables = time.time()
    insert_tables(cur, conn)
    end_insert_tables = time.time()
    print("Inserting data into star schema took {} seconds".format(end_insert_tables - start_insert_tables))

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()