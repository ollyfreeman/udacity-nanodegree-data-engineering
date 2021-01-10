from sql_queries import copy_table_queries, insert_table_queries
from utils import connect_to_database


def load_staging_tables(cur, conn):
    '''
    Loads data from S3 into the staging tables.

    Parameters:
        cur: Cursor of the sparkifydb database
        conn: Connection to the sparkifydb database

    Returns:
        None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Loads data from the staging tables into the regular tables.

    Parameters:
        cur: Cursor of the sparkifydb database
        conn: Connection to the sparkifydb database

    Returns:
        None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Script entry point:
    - Establishes connection with the sparkifydb database and gets cursor to it
    - Loads staging tables
    - Inserts data into regular tables from staging tables
    - Finally, closes the connection

    Parameters:
        None

    Returns:
        None
    '''
    cur, conn = connect_to_database()

    print('Loading staging tables')
    load_staging_tables(cur, conn)

    print('Inserting data into tables from staging tables')
    insert_tables(cur, conn)

    print('Done')
    conn.close()


if __name__ == "__main__":
    main()