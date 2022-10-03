import configparser
import time
from dataclasses import dataclass
from mysql.connector import Error
import mysql.connector

class DBConnection(object):
    
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance 

    def __init__(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self.database = config["database"]["database"]
        self.host = config["database"]["host"]
        self.port = config["database"]["port"]
        self.account = config["database"]["account"]
        self.password = config["database"]["password"]
        self.connection = None
        self.cursor = None

    def get_connnection(self):

        start_time = time.time()

        connection = mysql.connector.connect(host=self.host,
                                                database=self.database,
                                                user=self.account,
                                                password=self.password, connection_timeout=180)
        print("start...")
        try:
            if connection.is_connected():
                self.connection = connection
                self.cursor = connection.cursor()
                self.cursor.execute("select database();")
                record = self.cursor.fetchone()
                print(f"cost {time.time() - start_time}s to connect to DB")
                print("You're connected to database: ", record)

        except Error as e:
            print(e)
            self.connection.close()
            self.cursor.close()
            print("MySQL connection is closed")
