import cassandra.cluster
import os
import glob
import csv
import csql_queries


def create_denormalized_csv():
    '''
    Creates a denormalized CSV file from the raw event_data files in the `/event_data` directory.
  
    Returns the filename of the denormalized CSV file.

    Parameters:
        None

    Returns:
        None
    '''
    denormalized_csv_filename = 'event_datafile_new.csv'

    # Get current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'

    # Create a for loop to create a list of files and collect each filepath
    for root, _, __ in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 

    # for every filepath in the file path list 
    for f in file_path_list:

        # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

            # extracting each data row one by one and append it        
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line)

    # creating a smaller event data csv file
    # that will be used to insert data into the Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(denormalized_csv_filename, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow([
            'artist', 'firstName', 'gender', 'itemInSession', 'lastName',
            'length', 'level', 'location', 'sessionId', 'song', 'userId',
        ])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((
              row[0], row[2], row[3], row[4], row[5],
              row[6], row[7], row[8], row[12], row[13], row[16],
            ))

    return denormalized_csv_filename


def delete_denormalized_csv(denormalized_csv_filename):
    '''
    Deletes the denormalized CSV file.

    Parameters:
        denormalized_csv_filename: Filename of the denormalized CSV file

    Returns:
        None
    '''
    os.remove(denormalized_csv_filename)


def insert_rows(session, denormalized_csv_filename, insert_query_tuples):
    '''
    For each tuple in `insert_query_and_line_extractors`,
    executes the insert query using data extracted with the lambda funtion on eacah line in
    the denormalized CSV with filename `denormalized_csv_filename`

    Parameters:
        session: Current Cassandra session
        denormalized_csv_filename: Filename of the denormalized CSV file
        insert_query_tuples: List of tuples of the form:
          (CSQL query to insert data into table, lambda function to extract data from CSV row)

    Returns:
        None
    '''
    with open(denormalized_csv_filename, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        line_count = 0
        for line in csvreader:
            for insert_query, line_extractor in insert_query_tuples:
                session.execute(insert_query, line_extractor(line))
            line_count += 1
            if line_count % 100 == 0:
                print(f'{line_count} lines processed') 

def main():
    '''
    Script entry point:
    - Extracts, transforms and loads data from `event_data` CSV files
    into tables in the sparkify Cassandra keyspace.

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

    # create denormalized CSV
    denormalized_csv_filename = create_denormalized_csv()

    # insert data
    insert_query_tuples = [
        (
            csql_queries.sessions_table_insert,
            lambda line: (int(line[8]), int(line[3]), line[0], line[9], float(line[5]))
        ),
        (
            csql_queries.user_sessions_table_insert,
            lambda line: (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4])
        ),
        (
            csql_queries.song_listeners_table_insert,
            lambda line: (line[9], int(line[10]), line[1], line[4])
        )
    ]
    insert_rows(session, denormalized_csv_filename, insert_query_tuples)

    # delete denormalized CSV
    delete_denormalized_csv(denormalized_csv_filename)

    # shut down session and cluster
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
