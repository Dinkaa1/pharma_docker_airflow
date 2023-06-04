import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
import os

args = {
    'owner': 'Dinara',
    'start_date':datetime.datetime(2023, 5, 11),
    'provide_context':True
}

# Collecting Products
def extract_prods(**kwargs):
    ti = kwargs['ti']
    print("Start export prods")

    prods = pd.read_excel("dags/Products.xlsx",  engine='openpyxl')
    prods = prods.rename(columns={"SKU Code": "sku_code", "Short Brand": "short_brand", "Portfolio Type": "portfolio_type" })
    ti.xcom_push(key='prods', value=prods)

# Collecting PL Struct
def extract_pl_struct(**kwargs):
    ti = kwargs['ti']
    print("Start export pl struct")
    df_pl_struct = pd.read_excel("dags/PL Structure.xlsx",  engine='openpyxl', skipfooter=2)
    df_pl_struct = df_pl_struct.rename(columns={"GL Code SAP": "gl_code_sap", "PL Structure - Level 1": "pl_structure_level_1", "PL Structure - Level 2": "pl_structure_level_2", "PL Structure - Level 3": "pl_structure_level_3"})
    ti.xcom_push(key='pl_struct', value=df_pl_struct)

# Collecting data from test files and specifying their source
def extract_data(**kwargs):
    ti = kwargs['ti']

    #COGS 
    df_cogs = pd.read_excel("dags/COGS.xlsx", engine='openpyxl', skipfooter=3)
    df_cogs['source'] = 'COGS.xlsx'

    #Duties & Discounts 
    df_dd = pd.read_excel("dags/Duties & Discounts.xlsx", engine='openpyxl',  skipfooter=3)
    df_dd['source'] = 'Duties & Discounts.xlsx'

    #Logistics
    df_log = pd.read_excel("dags/Logistics.xlsx", engine='openpyxl', skipfooter=3)
    df_log['source'] = 'Logistics.xlsx'

    #Selling Costs
    df_scost = pd.read_excel("dags/Selling Costs.xlsx", engine='openpyxl', skipfooter=3)
    df_scost['source'] = 'Selling Costs.xlsx'
    #Sales
    df_sales = pd.read_excel("dags/Sales.xlsx", engine='openpyxl', skipfooter=3)
    df_sales['GL Code SAP'] = 'Sales code'
    df_sales['source'] = 'Sales.xlsx'
    df_sales = df_sales.rename(columns={"Sales": "Sum"})

    df_full_data = pd.concat([df_cogs, df_dd, df_log, df_scost, df_sales])
    df_full_data = df_full_data.rename(columns={"Sum": "sum", "GL Code SAP": "gl_code_sap", "Month": "month", "Year": "year", "SKU Code": "sku_code"})
    ti.xcom_push(key='source_data', value=df_full_data)

def extract_ap_brand(**kwargs):
    ti = kwargs['ti']

    #A&P by Brand 
    df_ap = pd.read_excel("dags/A&P by Brand.xlsx", engine='openpyxl', skipfooter=3)
    df_ap['source'] = 'A&P by Brand.xlsx'
    
    df_ap = df_ap.rename(columns={"Sum": "sum", "GL Code SAP": "gl_code_sap", "Month": "month", "Year": "year", "Short Brand": "short_brand"})
    ti.xcom_push(key='ap_brand', value=df_ap)

def transform_pl_struct(**kwargs):
    ti = kwargs['ti']
    pl_struct = ti.xcom_pull(key='pl_struct', task_ids=['extract_pl_struct'])[0]
    pl_struct = pl_struct.drop(pl_struct[pd.isna(pl_struct['gl_code_sap'])].index)
    ti.xcom_push(key='transformed_pl', value=pl_struct)

def transform_prods(**kwargs):
    ti = kwargs['ti']
    prods = ti.xcom_pull(key='prods', task_ids=['extract_prods'])[0]
    prods['st_date_portfolio'] = datetime.datetime.now()
    prods['end_date_portfolio'] = datetime.datetime(9999, 1, 1, 1, 1, 24)
    ti.xcom_push(key='transformed_prod', value=prods)

def transform_data(**kwargs):
    ti = kwargs['ti']
    full_data = ti.xcom_pull(key='source_data', task_ids=['extract_data'])[0]
    pl_struct = ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0]
    df_prods = ti.xcom_pull(key='transformed_prod', task_ids=['transform_prods'])[0]
    sales_code = pl_struct[pl_struct['pl_structure_level_1'] == 'Gross sales']['gl_code_sap'][0]
    full_data[full_data['gl_code_sap'] == 'Sales code']['gl_code_sap'] = sales_code

    df_sales = full_data[full_data['gl_code_sap'] == 'Sales code']
    df_sales_brands = df_sales.merge(df_prods, how='inner', on='SKU Code')
    df_sales_brands['prop'] = df_sales_brands.Sum / df_sales_brands.groupby(['Short Brand', 'Year', 'Month']).transform('sum').Sum
    df_ap = ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0]

    df_ap_sku = df_sales_brands.merge(df_ap, how='inner', on=['short_brand', 'year', 'month'])
    df_ap_sku['sum'] = df_ap_sku['sum'] * df_ap_sku['prop']
    df_ap_sku = df_ap_sku[['sum', 'year', 'month', 'gl_code_sap', 'sku_code']]
    full_data = pd.concat([full_data, df_ap_sku])
    full_data['sku_code'] = full_data['sku_code'].str.extract(r"(?:SKU[_-])?(\d+)")
    ti.xcom_push(key='transformed_full_data', value=full_data)

def calendar_dim(**kwargs):
    ti = kwargs['ti']
    months = list(range(1, 13))
    quarter_dict = {1: 1, 2: 1, 3: 1,
                    4: 2, 5: 2, 6: 2,
                    7: 3, 8: 3, 9: 3,
                    10: 4, 11: 4, 12: 4}

    calendar_df = pd.DataFrame({'month': months})
    calendar_df['half_year'] = calendar_df['month'].apply(lambda x: 1 if x <= 6 else 2)
    calendar_df['quarter'] = calendar_df['month'].map(quarter_dict)
    ti.xcom_push(key='calendar', value=calendar_df)

def fact_tbl(**kwargs):
    ti = kwargs['ti']
    full_data = ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0]
    pl_struct = ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0]

    merge_res = pl_struct.merge(full_data, how='inner', on='GL Code SAP')
    gross_sales = merge_res[merge_res['pl_structure_level_1'] == 'Gross sales'].groupby(['month', 'year', 'sku_code'], as_index=False).Sum.sum()
    duties = merge_res[merge_res['pl_structure_level_1'] == 'Duties'].groupby(['month', 'year', 'sku_code'], as_index=False).Sum.sum()
    discounts = merge_res[merge_res['pl_structure_level_1'] == 'Discounts'].groupby(['month', 'year', 'sku_code'], as_index=False).Sum.sum()

    nsv_prep = gross_sales.merge(duties, how='outer', on=['month', 'sku_code', 'year']).merge(discounts, how='outer', on=['month', 'sku_code', 'year'])
    nsv_prep['NSV Sum'] = nsv_prep.Sum_x - nsv_prep.Sum_y - nsv_prep.Sum
    nsv = nsv_prep[['year', 'month', 'sku_code', 'NSV_sum']]
    nsv = nsv.rename(columns={'NSV_sum': 'sum'})
    nsv = nsv[nsv.Sum.isnull() == False]
    nsv['gl_code_sap'] = 1

    sell_cost = merge_res[merge_res['pl_structure_level_1'] == 'Selling Cost'].groupby(['month', 'year', 'sku_code'], as_index=False).Sum.sum()

    tnsv_prep = sell_cost.merge(nsv, how='outer', on=['month', 'sku_code', 'year'])
    tnsv_prep['NSV Sum'] = tnsv_prep.Sum_x - tnsv_prep.Sum_y
    tnsv = tnsv_prep[['year', 'month', 'sku_code', 'NSV Sum']]
    tnsv = tnsv.rename(columns={'NSV Sum': 'sum'})
    tnsv = tnsv[tnsv.Sum.isnull() == False]

    tnsv['gl_code_sap'] = pl_struct[pl_struct['pl_structure_level_1'] == '3NSV']['gl_code_sap'].values[0]

    cogs = merge_res[merge_res['pl_structure_level_1'] == 'COGS'].groupby(['month', 'year', 'sku_code'], as_index=False).Sum.sum()
    logs = merge_res[merge_res['pl_structure_level_1'] == 'Logistics'].groupby(['month', 'year', 'sku_code'], as_index=False).Sum.sum()
    gross_prof_prep = cogs.merge(tnsv, how='outer', on=['month', 'sku_code', 'year']).merge(logs, how='outer', on=['month', 'sku_code', 'year'])
    gross_prof_prep['GP Sum'] = gross_prof_prep.Sum - gross_prof_prep.Sum_x - gross_prof_prep.Sum_y

    gross_prof = gross_prof_prep[['year', 'month', 'sku_code', 'GP Sum']]
    gross_prof = gross_prof.rename(columns={'GP Sum': 'sum'})
    gross_prof = gross_prof[gross_prof.Sum.isnull() == False]

    gross_prof['GL Code SAP'] = pl_struct[pl_struct['pl_structure_level_1'] == 'Gross Profit']['gl_code_sap'].values[0]

    ap =  merge_res[merge_res['pl_structure_level_1'] == 'A&P'].groupby(['month', 'year', 'sku_code'], as_index=False).Sum.sum()
    st_contr_prep = ap.merge(gross_prof, how='outer', on=['month', 'sku_code', 'year'])
    st_contr_prep['ST Sum'] = st_contr_prep.Sum_x - st_contr_prep.Sum_y

    st_contr = st_contr_prep[['year', 'Mmonth', 'sku_code', 'ST Sum']]
    st_contr = st_contr.rename(columns={'ST Sum': 'sum'})
    st_contr = st_contr[st_contr.Sum.isnull() == False]

    st_contr['GL Code SAP'] = pl_struct[pl_struct['pl_structure_level_1'] == 'ST Contribution']['gl_code_sap'].values[0]

    df_full_data = pd.concat([full_data,tnsv, nsv, gross_prof, st_contr])
    ti.xcom_push(key='dds_data', value=df_full_data)
    
with DAG('pharma_dwh', description='load_pharma', schedule_interval='*/1 * * * *',  catchup=False,default_args=args) as dag: 
        extract_start = BashOperator(task_id='extraction_start',bash_command='date')
        # Staging
        extract_data      = PythonOperator(task_id='extract_data', python_callable=extract_data)
        extract_pl_struct = PythonOperator(task_id='extract_pl_struct', python_callable=extract_pl_struct)
        extract_prods     = PythonOperator(task_id='extract_prods', python_callable=extract_prods)
        extract_ap     = PythonOperator(task_id='extract_ap', python_callable=extract_ap_brand)

        stg_create = BashOperator(task_id='staging_start',bash_command='date')
        create_pl_struct_tbl = PostgresOperator(
                task_id="create_pl_struct_tbl",
                postgres_conn_id="database_PG",
                search_path= "staging",
                sql="""
                    CREATE TABLE IF NOT EXISTS pl_struct
                    (
                        gl_code_sap character varying(255) COLLATE pg_catalog."default" NOT NULL,
                        pl_structure_level_1 character varying(255) COLLATE pg_catalog."default",
                        pl_structure_level_2 character varying(255) COLLATE pg_catalog."default",
                        pl_structure_level_3 character varying(255) COLLATE pg_catalog."default",
                        CONSTRAINT pl_struct_gl_code_sap_key UNIQUE (gl_code_sap)
                    );""")
        
        create_prods_tbl = PostgresOperator(
                task_id="create_prods_tbl",
                postgres_conn_id="database_PG",
                search_path= "staging",
                sql="""
                    CREATE TABLE IF NOT EXISTS.prod
                    (
                        sku_code integer NOT NULL,
                        short_brand character varying(255) COLLATE pg_catalog."default",
                        brand_portfolio_type character varying(255) COLLATE pg_catalog."default",
                        CONSTRAINT prod_sku_code_key UNIQUE (sku_code)
                    ); """)
                        
        create_stg_data_tbl = PostgresOperator(
                task_id="create_stg_data_tbl",
                postgres_conn_id="database_PG",
                search_path= "staging",
                sql="""
                    CREATE TABLE IF NOT EXISTS stg_data
                    (
                        gl_code_sap character varying(255) COLLATE pg_catalog."default",
                        sum integer,
                        year integer,
                        month integer,
                        sku_code integer,
                        source character varying(255)
                    ); """)
        
        create_stg_ap_brand_tbl = PostgresOperator(
                task_id="create_stg_ap_brand_tbl",
                postgres_conn_id="database_PG",
                search_path= "staging",
                sql="""
                    CREATE TABLE IF NOT EXISTS stg_ap_brand
                    (
                        gl_code_sap character varying(255) COLLATE pg_catalog."default",
                        sum integer,
                        year integer,
                        month integer,
                        short_brand integer,
                        source character varying(255)
                    ); """)
        
        stg_insert = BashOperator(task_id='staging_load',bash_command='date')
        insert_pl_struct = PostgresOperator(
                task_id="insert_pl",
                postgres_conn_id="database_PG",
                search_path= "staging",
                sql=[f"""INSERT INTO pl_struct VALUES(
                    {{{{ti.xcom_pull(key='pl_struct', task_ids=['extract_pl_struct'])[0].iloc[{i}]['gl_code_sap']}}}},
                '{{{{ti.xcom_pull(key='pl_struct', task_ids=['extract_pl_struct'])[0].iloc[{i}]['pl_structure_level_1']}}}}',
                '{{{{ti.xcom_pull(key='pl_struct', task_ids=['extract_pl_struct'])[0].iloc[{i}]['pl_structure_level_2']}}}}',
                '{{{{ti.xcom_pull(key='pl_struct', task_ids=['extract_pl_struct'])[0].iloc[{i}]['pl_structure_level_3']}}}}')
                """ for i in range(5)]
                )
        
        insert_prods = PostgresOperator(
                task_id="insert_prods",
                postgres_conn_id="database_PG",
                search_path= "staging",
                sql=[f"""INSERT INTO product VALUES(
                    {{{{ti.xcom_pull(key='prods', task_ids=['extract_prods'])[0].iloc[{i}]['sku_code']}}}},
                    '{{{{ti.xcom_pull(key='prods', task_ids=['extract_prods'])[0].iloc[{i}]['short_brand']}}}}',
                    '{{{{ti.xcom_pull(key='prods', task_ids=['extract_prods'])[0].iloc[{i}]['portfolio_type']}}}}'
                """ for i in range(4)]
                )
        
        insert_ap = PostgresOperator(
                task_id="insert_ap",
                postgres_conn_id="database_PG",
                search_path= "staging",
                sql=[f"""INSERT INTO stg_ap_brand VALUES(
                    {{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['gl_code_sap']}}}},
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['sum']}}}}',
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['year']}}}}',
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['month']}}}}',
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['short_brand']}}}}',
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['source']}}}}'
                """ for i in range(7)]
                )

        insert_data = PostgresOperator(
                task_id="insert_data",
                postgres_conn_id="database_PG",
                search_path= "staging",
                sql=[f"""INSERT INTO stg_data VALUES(
                    {{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['gl_code_sap']}}}},
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['sum']}}}}',
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['year']}}}}',
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['month']}}}}',
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['sku_code']}}}}',
                    '{{{{ti.xcom_pull(key='ap_brand', task_ids=['extract_ap_brand'])[0].iloc[{i}]['source']}}}}'
                """ for i in range(6)])

        # ODS
        ods_transform = BashOperator(task_id='ods_transform',bash_command='date')
        transform_pl_struct = PythonOperator(task_id='transform_pl_struct', python_callable=transform_pl_struct)
        transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
        
        ods_start = BashOperator(task_id='ods_start',bash_command='date')
        create_ods_pl_struct_tbl = PostgresOperator(
                task_id="create_ods_pl_struct_tbl",
                postgres_conn_id="database_PG",
                search_path= "ods",
                sql="""
                    CREATE TABLE IF NOT EXISTS pl_struct
                    (
                        gl_code_sap character varying(255) COLLATE pg_catalog."default" NOT NULL,
                        pl_structure_level_1 character varying(255) COLLATE pg_catalog."default",
                        pl_structure_level_2 character varying(255) COLLATE pg_catalog."default",
                        pl_structure_level_3 character varying(255) COLLATE pg_catalog."default",
                        CONSTRAINT pl_struct_gl_code_sap_key UNIQUE (gl_code_sap)
                    );""")
        
        create_ods_prods_tbl = PostgresOperator(
                task_id="create_ods_prods_tbl",
                postgres_conn_id="database_PG",
                search_path= "ods",
                sql="""
                    CREATE TABLE IF NOT EXISTS prod
                    (
                        sku_code integer NOT NULL,
                        short_brand character varying(255) COLLATE pg_catalog."default",
                        brand_portfolio_type character varying(255) COLLATE pg_catalog."default",
                        CONSTRAINT prod_sku_code_key UNIQUE (sku_code)
                    ); """)
                        
        create_ods_data_tbl = PostgresOperator(
                task_id="create_ods_data_tbl",
                postgres_conn_id="database_PG",
                search_path= "ods",
                sql="""
                    CREATE TABLE IF NOT EXISTS ods_data
                    (
                        gl_code_sap character varying(255) COLLATE pg_catalog."default",
                        sum integer,
                        year integer,
                        month integer,
                        sku_code integer
                    ); """)

        ods_load = BashOperator(task_id='ods_load',bash_command='date')
        insert_ods_pl_struct = PostgresOperator(
                task_id="insert_ods_pl_struct",
                postgres_conn_id="database_PG",
                search_path= "ods",
                sql=[f"""INSERT INTO pl_struct VALUES(
                    {{{{ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0].iloc[{i}]['gl_code_sap']}}}},
                '{{{{ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0].iloc[{i}]['pl_structure_level_1']}}}}',
                '{{{{ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0].iloc[{i}]['pl_structure_level_2']}}}}',
                '{{{{ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0].iloc[{i}]['pl_structure_level_3']}}}}')
                """ for i in range(5)]
                )
        
        insert_ods_prods = PostgresOperator(
                task_id="insert_ods_prods",
                postgres_conn_id="database_PG",
                search_path= "ods",
                sql=[f"""INSERT INTO prod VALUES(
                    {{{{ti.xcom_pull(key='transformed_prod', task_ids=['transform_prods'])[0].iloc[{i}]['sku_code']}}}},
                    '{{{{ti.xcom_pull(key='transformed_prod', task_ids=['transform_prods'])[0].iloc[{i}]['short_brand']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_prod', task_ids=['transform_prods'])[0].iloc[{i}]['portfolio_type']}}}}'
                """ for i in range(4)]
                )

        insert_ods_data = PostgresOperator(
                task_id="insert_ods_data",
                postgres_conn_id="database_PG",
                search_path= "ods",
                sql=[f"""INSERT INTO ods_data VALUES(
                    {{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['gl_code_sap']}}}},
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['sum']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['year']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['month']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['sku_code']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['source']}}}}'
                """ for i in range(7)])
        
        # DDS
        dds_start = BashOperator(task_id='dds_load',bash_command='date')
        calendar_dim = PythonOperator(task_id='calendar_dim', python_callable=calendar_dim)

        create_dds_dim_pl = PostgresOperator(
                task_id="create_dds_dim_pl",
                postgres_conn_id="database_PG",
                search_path= "dds",
                sql="""
                    CREATE TABLE IF NOT EXISTS dim_pl
                    (
                        gl_code_sap character varying(255) COLLATE pg_catalog."default" NOT NULL,
                        pl_structure_level_1 character varying(255) COLLATE pg_catalog."default",
                        pl_structure_level_2 character varying(255) COLLATE pg_catalog."default",
                        pl_structure_level_3 character varying(255) COLLATE pg_catalog."default",
                        CONSTRAINT pl_struct_gl_code_sap_key UNIQUE (gl_code_sap)
                    );""")
        
        create_dds_dim_prods = PostgresOperator(
                task_id="create_dds_dim_prods",
                postgres_conn_id="database_PG",
                search_path= "dds",
                sql="""
                    CREATE TABLE IF NOT EXISTS dim_prods
                    (
                        sku_code integer NOT NULL,
                        short_brand character varying(255) COLLATE pg_catalog."default",
                        brand_portfolio_type character varying(255) COLLATE pg_catalog."default",
                        CONSTRAINT prod_sku_code_key UNIQUE (sku_code)
                    ); """)
                        
        create_dds_fact_tbl = PostgresOperator(
                task_id="create_dds_fact_tbl",
                postgres_conn_id="database_PG",
                search_path= "dds",
                sql="""
                    CREATE TABLE IF NOT EXISTS fact_tbl
                    (
                        gl_code_sap character varying(255) COLLATE pg_catalog."default",
                        sum integer,
                        year integer,
                        month integer,
                        sku_code integer
                    ); """)
        
        create_dds_dim_calendar = PostgresOperator(
                task_id="create_dds_dim_calendar",
                postgres_conn_id="database_PG",
                search_path= "dds",
                sql="""
                    CREATE TABLE IF NOT EXISTS dim_calendar
                    (
                        month integer,
                        half_year integer,
                        quarter integer
                    ); """)
        
        dds_load = BashOperator(task_id='dds_load',bash_command='date')
        insert_dds_dim_pl = PostgresOperator(
                task_id="insert_dds_dim_pl",
                postgres_conn_id="database_PG",
                search_path= "dds",
                sql=[f"""INSERT INTO dim_pl VALUES(
                    {{{{ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0].iloc[{i}]['gl_code_sap']}}}},
                '{{{{ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0].iloc[{i}]['pl_structure_level_1']}}}}',
                '{{{{ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0].iloc[{i}]['pl_structure_level_2']}}}}',
                '{{{{ti.xcom_pull(key='transformed_pl', task_ids=['transform_pl_struct'])[0].iloc[{i}]['pl_structure_level_3']}}}}')
                """ for i in range(5)]
                )
        
        insert_dds_dim_prods = PostgresOperator(
                task_id="insert_dds_dim_prods",
                postgres_conn_id="database_PG",
                search_path= "ods",
                sql=[f"""INSERT INTO dim_prods VALUES(
                    {{{{ti.xcom_pull(key='transformed_prod', task_ids=['transform_prods'])[0].iloc[{i}]['sku_code']}}}},
                    '{{{{ti.xcom_pull(key='transformed_prod', task_ids=['transform_prods'])[0].iloc[{i}]['short_brand']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_prod', task_ids=['transform_prods'])[0].iloc[{i}]['portfolio_type']}}}}'
                """ for i in range(4)]
                )

        insert_dds_fact = PostgresOperator(
                task_id="insert_dds_fact",
                postgres_conn_id="database_PG",
                search_path= "dds",
                sql=[f"""INSERT INTO fact_tbl VALUES(
                    {{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['gl_code_sap']}}}},
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['sum']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['year']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['month']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['sku_code']}}}}',
                    '{{{{ti.xcom_pull(key='transformed_full_data', task_ids=['transform_data'])[0].iloc[{i}]['source']}}}}'
                """ for i in range(7)])
        
        insert_dds_dim_calendar = PostgresOperator(
                task_id="insert_dds_dim_calendar",
                postgres_conn_id="database_PG",
                search_path= "dds",
                sql=[f"""INSERT INTO dim_calendar VALUES(
                    '{{{{ti.xcom_pull(key='calendar', task_ids=['calendar_dim'])[0].iloc[{i}]['month']}}}}',
                    '{{{{ti.xcom_pull(key='calendar', task_ids=['calendar_dim'])[0].iloc[{i}]['half_year']}}}}',
                    '{{{{ti.xcom_pull(key='calendar', task_ids=['calendar_dim'])[0].iloc[{i}]['quarter']}}}}'
                """ for i in range(4)])
        
        # Data Mart
        dm_start = BashOperator(task_id='dm_start',bash_command='date')
        create_data_view = PostgresOperator(
                task_id="create_data_view",
                postgres_conn_id="database_PG",
                search_path= "data_mart",
                sql=(""" CREATE VIEW data_mart as
                         SELECT * from dds.fact_tbl
                         LEFT JOIN dds.dim_pl on dds.fact_tbl.sku_code = dds.dim_pl.sku_code
                         LEFT JOIN dds.dim_pl on dds.dim_pl.gl_code_sap = dds.fact_tbl.gl_code_sap
                         LEFT JOIN dds.dim_calendar on dds.dim_calendar.month = dds.fact_tbl.month
                """
                ))
        
        extract_start >> [extract_pl_struct, extract_data, extract_prods, extract_ap] >> stg_create
        stg_create >> [create_prods_tbl, create_pl_struct_tbl, create_stg_data_tbl, create_stg_ap_brand_tbl]  >> stg_insert
        stg_insert >> [insert_pl_struct, insert_prods, insert_ap,  insert_data]  >> ods_transform
        ods_transform >> [transform_pl_struct, transform_data] >> ods_start
        ods_start >> [create_ods_pl_struct_tbl, create_ods_prods_tbl, create_ods_data_tbl] >> ods_load
        ods_load >> [insert_ods_pl_struct, insert_ods_prods, insert_ods_data] >> dds_start
        dds_start >> [calendar_dim, create_dds_dim_pl, create_dds_dim_prods, create_dds_fact_tbl, create_dds_dim_calendar] >> dds_load
        dds_load >> [insert_dds_dim_pl, insert_dds_dim_prods, insert_dds_fact, insert_dds_dim_calendar] >> dm_start
        dm_start >> create_data_view

        