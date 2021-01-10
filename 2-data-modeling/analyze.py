import cassandra.cluster
import pandas as pd
import csql_queries

def print_query(session, query_name):
    '''
    Executes the CSQL query referenced by query_name, and prints the results

    Parameters:
        session: Current Cassandra session
        query_name: Human-friendly name of CSQL query

    Returns:
        None
    '''
    print(f'\n{query_name}')
    print(pd.DataFrame(list(session.execute(csql_queries.queries[query_name]))))


def main():
    '''
    Script entry point:
    - Queries the data in the sparkify Cassandra keyspace, and prints the results

    Parameters:
        None

    Returns:
        None
    '''
    # connect to cluster
    cluster = cassandra.cluster.Cluster(['127.0.0.1'])
    session = cluster.connect()

    # set current keyspace for session
    session.set_keyspace('sparkify')

    # query data
    ## sessions for query 1
    print_query(session, 'Query 1')

    ## user_sessions for query 2 
    print_query(session, 'Query 2')

    ## song_listeners for query 3
    print_query(session, 'Query 3')

    # shut down session and cluster
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
