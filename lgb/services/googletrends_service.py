from services.service import Service
class GoogleTrends(Service):
    def __init__(self, cursor):
        super().__init__(cursor, "googletrends")

    def get_dataInfo(self):
        sql_query = f"select * from {self.name}"
        self.cursor.execute(sql_query)
        records = self.cursor.fetchall()
        print(f"Total number of rows in table {self.name}: ", self.cursor.rowcount)



