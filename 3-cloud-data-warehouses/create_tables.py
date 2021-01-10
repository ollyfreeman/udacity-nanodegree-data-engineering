from sql_queries import create_table_queries, drop_table_queries
from utils import connect_to_database


def drop_tables(cur, conn):
    '''
    Drops each table using the queries in `drop_table_queries` list.

    Parameters:
        cur: Cursor of the sparkifydb database
        conn: Connection to the sparkifydb database

    Returns:
        None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates each table using the queries in `create_table_queries` list. 

    Parameters:
        cur: Cursor of the sparkifydb database
        conn: Connection to the sparkifydb database

    Returns:
        None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Script entry point:
    - Establishes connection with the sparkifydb database and gets cursor to it
    - Drops all the tables
    - Creates all the tables
    - Finally, closes the connection

    Parameters:
        None

    Returns:
        None
    '''
    cur, conn = connect_to_database()

    print('Dropping tables')
    drop_tables(cur, conn)

    print('Creating tables')
    create_tables(cur, conn)

    print('Done')
    conn.close()


if __name__ == '__main__':
    main()
