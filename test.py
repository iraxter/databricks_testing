from databricks import sql
import os
import yaml

with open('creds.yaml', 'r') as f:
    creds = yaml.safe_load(f)

with sql.connect(server_hostname = creds['test_hostname'],
                 http_path = creds['test_http_path'],
                 access_token = creds['test_access_token']) as connection:
    
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS squares (x int, x_squared int)")

        squares = [(i, i * i) for i in range(100)]
        values = ",".join([f"({x}, {y})" for (x, y) in squares])

        cursor.execute(f"INSERT INTO squares VALUES {values}")

        cursor.execute("SELECT * FROM squares LIMIT 10")

        result = cursor.fetchall()

    for row in result:
      print(row)