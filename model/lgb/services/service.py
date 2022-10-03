class Service:
    def __init__(self, cursor, name):
        self.cursor = cursor
        self.name = name
    
    def get_dataInfo(self):
        pass
        # sql_query = f"select * from {self.name}"
        # self.cursor.execute(sql_query)
        # records = self.cursor.fetchall()
        # print("Total number of rows in table: ", self.cursor.rowcount)