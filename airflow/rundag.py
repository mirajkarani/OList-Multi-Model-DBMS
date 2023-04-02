from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from projectfiles.cosmosdbqueries import top_100_prod
from projectfiles.cosmosdbqueries import top_100_cust
from projectfiles.cosmosdbqueries import seller_by_geolocation
from projectfiles.cosmosdbqueries import cust_payment_methods
from projectfiles.cosmosdbqueries import delivery_by_gelocation
from projectfiles.cosmosdbqueries import cust_order_reviews


args = {
    'owner': 'Aniket Mirajkar',
    'start_date': datetime(2023, 3, 31) 
}

dag = DAG(
    dag_id='damg-7275-project',
    default_args=args,
    schedule_interval='0 15 * * *'
)

op1 = PythonOperator(
        task_id='top-100-prod',
        python_callable=top_100_prod,
        dag=dag
    )

op2 = PythonOperator(
        task_id='top-100-cust',
        python_callable=top_100_cust,
        dag=dag
    )     

op3 = PythonOperator(
        task_id='seller-by-location',
        python_callable=seller_by_geolocation,
        dag=dag
    )  

op4 = PythonOperator(
        task_id='customer-payment',
        python_callable=cust_payment_methods,
        dag=dag
    )

op5 = PythonOperator(
        task_id='delivery-by-location',
        python_callable=delivery_by_gelocation,
        dag=dag
    )

op6 = PythonOperator(
        task_id='cust-order-reviews',
        python_callable=cust_order_reviews,
        dag=dag
    )

op1 >> op2 >> op3 >> op4 >> op5 >> op6