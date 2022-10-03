from services.service import Service
class DTFBase(Service):
    def __init__(self, cursor):
        super().__init__(cursor, "dtf_base")
    
    def get_dataInfo(self, sql_query):
        sql_query = sql_query
        self.cursor.execute(sql_query)
        records = self.cursor.fetchall()
        print(f"Total number of rows in table {self.name}: ", self.cursor.rowcount)

        return records