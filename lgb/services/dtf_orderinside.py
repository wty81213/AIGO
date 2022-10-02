from services.service import Service

class DTFOrderInside(Service):
    def __init__(self, cursor):
        super().__init__(cursor, "dtf_orderinside")

    def get_dataInfo(self, sql_query, *args):
        sql_query = sql_query
        self.cursor.execute(sql_query, (args[0], args[0]))
        records = self.cursor.fetchall()
        print(f"Total number of rows in table {self.name}: ", self.cursor.rowcount)
        return records