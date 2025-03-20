from databricks import sql
from sqlalchemy import create_engine
import yaml
import pandas as pd

def read_credentials(creds_path):
    #Simple yaml read function to return credentials dictionary with all variables needed to connect to Databricks server. Requires creds.yaml file in the same directory (use the same format as creds_template.yaml).
    with open(creds_path, 'r') as f:
        creds = yaml.safe_load(f)
    return creds

def exec_sql(creds, sql_string):
    #Function to execute a SQL string on a Databricks server. Requires credentials dictionary built by read_credentials and a SQL query string.
    with sql.connect(server_hostname = creds['hostname'],
                     http_path = creds['http_path'],
                     access_token = creds['access_token']) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_string)
            result = cursor.fetchall()
    return result

def df_to_sql(creds, df, table, catalog=None, schema=None, if_exists='replace'):
    #Function to write a pandas dataframe as a table in Databricks. Requires credentials dictionary built by read_credentials, a csv file path, a table name, and (optional) catalog/schema override. If catalog or schema is not provided, the defaults from creds.yaml will be used.
    #if_exists variable matches Pandas to_sql args ('fail', 'replace', 'append'), default is 'replace'

    catalog = catalog if catalog else creds['default_catalog']
    schema = schema if schema else creds['default_schema']

    connstring = f"databricks://token:{creds['access_token']}@{creds['hostname']}?http_path={creds['http_path']}&catalog={catalog}&schema={schema}"
    engine = create_engine(connstring)

    with engine.connect() as conn:
        df.to_sql(table, conn, index=False, if_exists=if_exists)

def csv_to_sql(creds, csv, table, catalog=None, schema=None, if_exists='replace'):
    #Wrapper to read a csv into a pandas df then run df_to_sql to create or insert to a Databricks table. Requires credentials dictionary built by read_credentials, a csv file path, a table name, and (optional) catalog/schema override. If catalog or schema is not provided, the defaults from creds.yaml will be used.
    #if_exists variable matches Pandas to_sql args ('fail', 'replace', 'append'), default is 'replace'

    csv_df = pd.read_csv(csv)
    print(csv_df)
    df_to_sql(creds, csv_df, table, catalog, schema, if_exists)