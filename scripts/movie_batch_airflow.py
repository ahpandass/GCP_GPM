from datetime import datetime, timedelta, date
from airflow import models, DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

bucket = 'gs://shan_movie_bucket/'
clustername = 'spark-computing-tmp-cluster'

pyspark_job = bucket+ 'movie_etl.py'
output_file = 'movies.parquet/*.parquet'

default_DAG_args = {
    'owner' : 'shan-airflow',
    'depends_on_past' : False,
    'start_date':datetime(2021,5,18,0,0),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=0),
    'project_id':'shan-bigdata',
    'scheduled_interval':"22 * * * *"
}

with DAG('comedy_movie_process',default_args=default_DAG_args, catchup=False) as dag:

    create_cluster = DataprocClusterCreateOperator(
        task_id = "cluster_create",
        cluster_name=clustername,
        master_machine_type="n1-standard-2",
        worker_marchine_type="n1-standard-2",
        num_workers=2,
        region='asia-southeast1',
        zone='asia-southeast1-a'        
    )
    
    submit_pyspark = DataProcPySparkOperator(
        task_id="load_data_into_parquet",
        main=pyspark_job,
        cluster_name=clustername,
        region='asia-southeast1'
    )
    
    export_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="export_transactions_parquet_to_bq",
        bucket="shan_movie_bucket",
        source_objects=[output_file],
        destination_project_dataset_table="movie_sample.comedy_movie",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
        )
    
    comedy_process = BigQueryOperator(
        task_id="process_comedy_movies",
        sql=open('/home/airflow/gcs/data/comedy_movie_sp.bql','r').read(),
        bigquery_conn_id='bigquery_default',
        allow_large_results=True,
        use_legacy_sql=False,
        create_disposition='CREATE_IF_NEEDED',
        dag=dag
    )
    
    delete_cluster = DataprocClusterDeleteOperator(
        task_id="cluster_delete",
        cluster_name=clustername,
        region='asia-southeast1',
        trigger_rule=TriggerRule.ALL_DONE
    )
    create_cluster>>submit_pyspark>>export_to_bq>>comedy_process>>delete_cluster
    #submit_pyspark>>export_to_bq
    #create_cluster.dag=dag
    #create_cluster.set_downstream(export_to_bq)
    #export_to_bq.set_downstream(submit_pyspark)
    #submit_pyspark.set_downstream(delete_cluster)
    