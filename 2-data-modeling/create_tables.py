import cassandra.cluster
from csql_queries import create_table_queries, drop_table_queries


def create_database():
    '''
    Connects to the local Cassandra cluster.

    Creates the sparkifydb keyspace (if it doesn't exist).
  
    Sets the keyspace of the session to sparkifydb.

    Parameters:
        None

    Returns:
        cluster: Local Cassandra cluster
        session: Current Cassandra session
    '''
    
    # connect to cluster
    cluster = cassandra.cluster.Cluster(['127.0.0.1'])
    session = cluster.connect()

    # create keyspace
    session.execute(
        '''
        CREATE KEYSPACE IF NOT EXISTS sparkify
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        '''
    )

    # set current keyspace for session
    session.set_keyspace('sparkify')

    return cluster, session


def drop_tables(session):
    '''
    Drops each table using the queries in `drop_table_queries` list.

    Parameters:
        session: Current Cassandra session

    Returns:
        None
    '''
    for query in drop_table_queries:
        session.execute(query)


def create_tables(session):
    '''
    Creates each table using the queries in `create_table_queries` list.

    Parameters:
        session: Current Cassandra session

    Returns:
        None
    '''
    for query in create_table_queries:
        session.execute(query)


def main():
    '''
    Script entry point:
    - Establishes connection to the local Cassandra cluster's sparkify keyspace  
    - Drops all the tables
    - Creates all tables needed
    - Finally, closes the connection

    Parameters:
        None

    Returns:
        None
    '''
    cluster, session = create_database()
    
    drop_tables(session)
    create_tables(session)

    # shut down session and cluster
    session.shutdown()
    cluster.shutdown()


if __name__ == '__main__':
    main()
