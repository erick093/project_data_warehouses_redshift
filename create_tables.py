import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables in the database
    :param cur: cursor
    :param conn: connection
    :return:
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all tables in the database
    :param cur: cursor
    :param conn: connection
    :return:
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting to Redshift...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Dropping existing tables...")
    drop_tables(cur, conn)

    print("Creating new tables...")
    create_tables(cur, conn)

    print("Tables created successfully!")
    conn.close()


if __name__ == "__main__":
    main()