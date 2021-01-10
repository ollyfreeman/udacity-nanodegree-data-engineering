import configparser
import psycopg2

def connect_to_database():
    '''
    Connects to the sparkifydb database

    Parameters:
        None

    Returns:
        cur: Cursor of the sparkifydb database
        conn: Connection to the sparkifydb database
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
  
    conn = psycopg2.connect('host={} dbname={} user={} password={} port={}'.format(
        config['CLUSTER']['HOST'],
        config['CLUSTER']['DB'],
        config['CLUSTER']['DB_USER'],
        config['CLUSTER']['DB_PASSWORD'],
        config['CLUSTER']['PORT']
    ))
    cur = conn.cursor()

    return cur, conn