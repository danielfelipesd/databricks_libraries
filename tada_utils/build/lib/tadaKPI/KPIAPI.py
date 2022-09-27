from snowflake.connector import connect

class KPIRunner:
    def __init__(self, query, queryParameters, dbutils, **kwargs):
        self.con = connect(
            user = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'SnowflakeUser'),
            password = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'SnowflakePassword'),
            account = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'SnowflakeAccount')
        )
        if kwargs.get("role"):
            self.role = kwargs.get("role")
        else:
            self.role = 'SYSADMIN'
        if kwargs.get("warehouse"):
            self.warehouse = kwargs.get("warehouse")
        else:
            self.warehouse = "WH_WORKLOAD"
        self.query = query.format(**queryParameters)
        
    def runQuery(self):
        try:
            print(self.query)
            cursor = self.con.cursor()
            cursor.execute(f"USE ROLE {self.role};")
            cursor.execute(f"USE WAREHOUSE {self.warehouse};")
            cursor.execute(self.query)
        except Exception as e:
            raise e
        cursor.close()