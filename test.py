from databricks import sql
from utils import *
import yaml

# Raw test code for reference, moving on to function test code below
'''
with open('creds.yaml', 'r') as f:
    creds = yaml.safe_load(f)

with sql.connect(server_hostname = creds['hostname'],
                 http_path = creds['http_path'],
                 access_token = creds['access_token']) as connection:
    
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS squares (x int, x_squared int)")

        squares = [(i, i * i) for i in range(100)]
        values = ",".join([f"({x}, {y})" for (x, y) in squares])

        cursor.execute(f"INSERT INTO squares VALUES {values}")

        cursor.execute("SELECT * FROM squares LIMIT 10")

        result = cursor.fetchall()

    for row in result:
      print(row)
'''

creds = read_credentials('creds.yaml')

csv_to_sql(creds, 'test_csv.csv', 'test_csv_table', if_exists='replace')

sql_string = f"SELECT * FROM {creds['default_catalog']}.{creds['default_schema']}.test_csv_table LIMIT 10"

result = exec_sql(creds, sql_string)

print(result)