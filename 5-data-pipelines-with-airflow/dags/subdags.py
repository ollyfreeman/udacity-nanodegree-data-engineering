from airflow import DAG
from airflow.operators import LoadDimensionOperator

def load_dimensional_tables_subdag(
        parent_dag_name,
        task_id,
        table,
        sql_select_query,
        mode='delete-load',
        *args, **kwargs,
):
    '''
    Creates a SubDAG that simply calls LoadDimensionOperator
  
    Returns the SubDAG

    Parameters:
        parent_dag_name: Name of the parent DAG of this subdag
        task_id: Task ID of this subdag
        table: SQL table to be loaded into
        sql_select_query: SQL query to select the data to be inserted
        mode: The mode of the load: 'append-only' or 'delete-load'

    Returns:
        The SubDAG
    '''
    dag = DAG(
        f'{parent_dag_name}.{task_id}',
        **kwargs,
    )

    LoadDimensionOperator(
        task_id=f"{task_id}__load",
        dag=dag,
        table=table,
        sql_select_query=sql_select_query,
        mode=mode
    )

    return dag
