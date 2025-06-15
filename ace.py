from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3

def extract():
    data = {
        'customer': pd.read_csv('/path/to/customer.csv'),
        'rental': pd.read_csv('/path/to/rental.csv'),
        'address': pd.read_csv('/path/to/address.csv'),
        'city': pd.read_csv('/path/to/city.csv'),
        'country': pd.read_csv('/path/to/country.csv'),
        'payment': pd.read_csv('/path/to/payment.csv'),
        'staff': pd.read_csv('/path/to/staff.csv'),
        'store': pd.read_csv('/path/to/store.csv')
    }
    return data

def transform(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_task')
    customer = pd.DataFrame(data['customer'])
    rental = pd.DataFrame(data['rental'])
    address = pd.DataFrame(data['address'])
    city = pd.DataFrame(data['city'])
    country = pd.DataFrame(data['country'])
    payment = pd.DataFrame(data['payment'])
    staff = pd.DataFrame(data['staff'])
    store = pd.DataFrame(data['store'])

    df = rental.join(customer, on='customer_id', how='left', lsuffix='customer', rsuffix='rental')\
             .join(address, on='address_id', how='left', lsuffix='customer', rsuffix='address')\
             .join(city, on='city_id', how='left', lsuffix='address', rsuffix='city')\
             .join(country, on='country_id', how='left', lsuffix='city', rsuffix='country')

    df = df.merge(store, on='store_id', how='left')\
           .merge(staff, on='store_id', how='left')\
           .merge(payment, on='customer_id', how='left')

    df = df.rename(columns={
        'address_id_x': 'address_id', 'rental_id_x': 'rental_id', 'first_name_x': 'first_name',
        'last_name_x': 'last_name', 'email_x': 'email', 'active_x': 'active',
        'activebool': 'active_bool', 'city_id': 'city_id1', 'city_idcity': 'city_id',
        'customer_idcustomer': 1, 'address_id_y': 2, 'last_update_y': 3,
        'last_updatecustomer': 4, 'customer_idrental': 5, 'address_idcustomer': 6,
        'last_updaterental': 7, 'address_idaddress': 8, 'city_idaddress': 7,
        'last_updateaddress': 9, 'country_idcity': 10, 'last_updatecity': 11,
        'country_idcountry': 12, 'staff_id_y': 13, 'first_name_y': 14,
        'last_name_y': 15, 'address_id': 16, 'email_y': 17, 'active_y': 18,
        'last_update_x': 19, 'staff_id_x': 20, 'rental_id_y': 21
    })

    df.drop(columns=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                     14, 15, 16, 17, 18, 19, 20, 21, 'address2', 'city_id1'], inplace=True)

    kwargs['ti'].xcom_push(key='transformed_df', value=df.to_json())

def load(**kwargs):
    df_json = kwargs['ti'].xcom_pull(task_ids='transform_task', key='transformed_df')
    df = pd.read_json(df_json)
    conn = sqlite3.connect('INFORMATION.db')
    df.to_sql('INFORMATION', conn, if_exists='replace', index=False)

    # SQL queries example
    # Query 1: Display first 10 rows of the table
    query = f'SELECT * FROM INFORMATION LIMIT 10'
    query_output = pd.read_sql(query, conn)

    # Query 2: Dispaly only the country column for the full table
    query = f'SELECT country FROM INFORMATION'
    query_output = pd.read_sql(query, conn)

    # Query 3: Total number of rows
    query = f'SELECT COUNT(*) FROM INFORMATION'
    query_output = pd.read_sql(query, conn)

    # Query 4: Total number of rows
    query = f'SELECT customer_id FROM INFORMATION WHERE customer_id = "5"'
    query_output = pd.read_sql(query, conn)

    # Close the connection
    conn.close()

with DAG(
    dag_id='sql_etl_airflow_pipeline',
    start_date=datetime(2024, 6, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True
    )

    extract_task >> transform_task >> load_task

