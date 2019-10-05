from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (LoadLocalFilesToS3Operator, CopyS3ToRedshiftOperator,
                               LoadDimFileDataOperator, LoadFinalTableOperator,
                               DataQualityCheckOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'vlemay',
    'start_date': datetime(2019, 9, 28),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False
}

base_file_path = '/Users/victorialemay/Courses/Udacity/Data Engineering Nanodegree/capstone/project-archive/airflow'

dag = DAG('capstone_project_dag',
          default_args=default_args,
          description='Load I94 and US city demographics data into Redshift with Airflow',
          schedule_interval=None,
          template_searchpath=base_file_path + '/sql'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

staging_table_load_operator = PostgresOperator(
    task_id='create_staging_database_tables',
    dag=dag,
    sql='create_staging_tables.sql',
    postgres_conn_id='redshift'
)

final_table_load_operator = PostgresOperator(
    task_id='create_final_database_tables',
    dag=dag,
    sql='create_final_tables.sql',
    postgres_conn_id='redshift'
)

stage_dim_data_files_to_s3 = LoadLocalFilesToS3Operator(
    task_id='Stage_dim_data_files',
    dag=dag,
    s3_bucket='project-capstone',
    directory=base_file_path + '/data/dim_table_data'
)

stage_sas_data_files_to_s3 = LoadLocalFilesToS3Operator(
    task_id='Stage_sas_data_parquet_files',
    dag=dag,
    s3_bucket='project-capstone-parquet',
    directory=base_file_path + '/data/sas_data'
)

stage_staging_data_files_to_s3 = LoadLocalFilesToS3Operator(
    task_id='Stage_staging_data_files',
    dag=dag,
    s3_bucket='project-capstone',
    directory=base_file_path + '/data/staging_data'
)

load_demographic_staging_table = CopyS3ToRedshiftOperator(
    task_id='Load_demographic_staging_table',
    dag=dag,
    table='demographic_staging',
    s3_bucket='project-capstone',
    s3_key='us-cities-demographics.csv',
    copy_format="CSV DELIMITER ';' IGNOREHEADER 1"
)

load_temperature_staging_table = CopyS3ToRedshiftOperator(
    task_id='Load_temperature_staging_table',
    dag=dag,
    table='land_temp_staging',
    s3_bucket='project-capstone',
    s3_key='GlobalLandTemperaturesByCity.csv',
    copy_format="CSV DELIMITER ',' IGNOREHEADER 1"
)

load_port_code_staging_table = CopyS3ToRedshiftOperator(
    task_id='Load_port_code_staging_table',
    dag=dag,
    table='port_code_staging',
    s3_bucket='project-capstone',
    s3_key='port_codes.csv',
    copy_format="CSV DELIMITER ',' IGNOREHEADER 1"
)

load_i94_immigration_staging_table = CopyS3ToRedshiftOperator(
    task_id='Load_i94_immigration_staging_table',
    dag=dag,
    table='i94_immigration_staging',
    s3_bucket='project-capstone-parquet',
    parquet_arn='arn:aws:iam::191711490296:role/myRedshiftRole'
)

load_travel_modes_dim_table = LoadDimFileDataOperator(
    task_id='Load_travel_modes_dim_table',
    dag=dag,
    table='travel_modes',
    s3_bucket='project-capstone',
    s3_key='travel_modes.json',
    copy_format="JSON 'auto'"
)

load_visa_types_dim_table = LoadDimFileDataOperator(
    task_id='Load_visa_types_dim_table',
    dag=dag,
    table='visa_types',
    s3_bucket='project-capstone',
    s3_key='visa_types.json',
    copy_format="JSON 'auto'"
)

load_us_states_dim_table = LoadDimFileDataOperator(
    task_id='Load_us_states_dim_table',
    dag=dag,
    table='us_states',
    s3_bucket='project-capstone',
    s3_key='us_states.csv',
    copy_format="CSV DELIMITER ',' IGNOREHEADER 1"
)

load_country_codes_dim_table = LoadDimFileDataOperator(
    task_id='Load_country_codes_dim_table',
    dag=dag,
    table='country_codes',
    s3_bucket='project-capstone',
    s3_key='country_codes.csv',
    copy_format="CSV DELIMITER ',' IGNOREHEADER 1"
)

run_staging_tables_quality_check = DataQualityCheckOperator(
    task_id='Run_staging_tables_quality_checks',
    dag=dag,
    tables={'i94_immigration_staging': 'cicid', 'land_temp_staging': 'dt',
            'demographic_staging': 'City', 'port_code_staging': 'name', 'travel_modes': 'id',
            'visa_types': 'id', 'us_states': 'id', 'country_codes': 'id'},
    check_functions=['has_zero_rows']
)

load_port_codes_final_table = LoadFinalTableOperator(
    task_id='Load_port_codes_final_table',
    dag=dag,
    table='port_codes',
    columns=['city', 'state_id', 'i94_port_code'],
    select_sql=SqlQueries.port_codes_table_insert
)

load_us_city_demographics_final_table = LoadFinalTableOperator(
    task_id='Load_us_city_demographics_final_table',
    dag=dag,
    table='us_city_demographics',
    columns=['port_code_id', 'city', 'median_age', 'male_pop', 'female_pop', 'total_pop',
             'num_vets', 'foreign_pop', 'avg_household_size', 'race', 'race_pop'],
    select_sql=SqlQueries.us_city_demographics_table_insert
)

load_land_temperatures_final_table = LoadFinalTableOperator(
    task_id='Load_land_temperatures_final_table',
    dag=dag,
    table='land_temperatures',
    columns=['date', 'avg_temp', 'avg_temp_uncertainty', 'city', 'country_code_id',
             'latitude', 'longitude'],
    select_sql=SqlQueries.land_temperatures_table_insert
)

load_i94_immigration_final_table = LoadFinalTableOperator(
    task_id='Load_i94_immigration_final_table',
    dag=dag,
    table='i94_immigration',
    columns=['year', 'month', 'res_country_code_id', 'port_id', 'travel_mode_id',
             'arrival_date', 'departure_date', 'respondent_age', 'visa_type_code_id',
             'visa_post_code_id', 'occupation', 'birth_year', 'gender', 'visa_type'],
    select_sql=SqlQueries.i94_immigration_table_insert
)

run_final_tables_quality_check = DataQualityCheckOperator(
    task_id='Run_final_tables_quality_checks',
    dag=dag,
    tables={'i94_immigration': 'port_id', 'port_codes': 'i94_port_code',
            'land_temperatures': 'date', 'us_city_demographics': 'port_code_id'},
    check_functions=['has_zero_rows', 'has_nulls']
)

load_i94_immigration_rollup_final_table = LoadFinalTableOperator(
    task_id='Load_i94_immigration_rollup_final_table',
    dag=dag,
    table='i94_immigration_by_year_month_port',
    columns=['year', 'month', 'city', 'state_code', 'i94_port_code',
             'total', 'total_men', 'total_women', 'avg_age'],
    select_sql=SqlQueries.i94_immigration_by_year_month_port_table_insert
)

end_operator = DummyOperator(task_id='Done_execution',  dag=dag)


start_operator >> stage_dim_data_files_to_s3
start_operator >> stage_sas_data_files_to_s3
start_operator >> stage_staging_data_files_to_s3

stage_sas_data_files_to_s3 >> staging_table_load_operator
stage_dim_data_files_to_s3 >> staging_table_load_operator
stage_staging_data_files_to_s3 >> staging_table_load_operator

stage_sas_data_files_to_s3 >> final_table_load_operator
stage_dim_data_files_to_s3 >> final_table_load_operator
stage_staging_data_files_to_s3 >> final_table_load_operator

staging_table_load_operator >> load_demographic_staging_table
staging_table_load_operator >> load_temperature_staging_table
staging_table_load_operator >> load_port_code_staging_table
staging_table_load_operator >> load_i94_immigration_staging_table

final_table_load_operator >> load_travel_modes_dim_table
final_table_load_operator >> load_visa_types_dim_table
final_table_load_operator >> load_us_states_dim_table
final_table_load_operator >> load_country_codes_dim_table

load_demographic_staging_table >> run_staging_tables_quality_check
load_temperature_staging_table >> run_staging_tables_quality_check
load_port_code_staging_table >> run_staging_tables_quality_check
load_i94_immigration_staging_table >> run_staging_tables_quality_check
load_country_codes_dim_table >> run_staging_tables_quality_check
load_visa_types_dim_table >> run_staging_tables_quality_check
load_travel_modes_dim_table >> run_staging_tables_quality_check
load_us_states_dim_table >> run_staging_tables_quality_check

run_staging_tables_quality_check >> load_port_codes_final_table
run_staging_tables_quality_check >> load_us_city_demographics_final_table
run_staging_tables_quality_check >> load_land_temperatures_final_table
run_staging_tables_quality_check >> load_i94_immigration_final_table

load_port_codes_final_table >> run_final_tables_quality_check
load_us_city_demographics_final_table >> run_final_tables_quality_check
load_land_temperatures_final_table >> run_final_tables_quality_check
load_i94_immigration_final_table >> run_final_tables_quality_check

run_final_tables_quality_check >> load_i94_immigration_rollup_final_table

load_i94_immigration_rollup_final_table >> end_operator
