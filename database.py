# -*- coding: utf-8 -*-

import pymongo
import psycopg2
from sqlalchemy import create_engine


class database:
    
    def __init__(self, host, user, password, port: int):
        self.__HOST = host
        self.__USER = user
        self.__PASSWORD = password
        self.__PORT = port
                

    def connectDB(self, dbname):
        '''
        Returns
        -------
        conn : TYPE
            connect trends DB 
        '''
        conn = psycopg2.connect(
            host = self.__HOST,
            dbname = dbname,
            user = self.__USER,
            password = self.__PASSWORD,
            port = self.__PORT
            )

        return conn
    
    
    def makeEngine(self, dbname, echo = True):
        '''
        Returns
        -------
        engine : connect
            connect trends DB 
        '''
        engine = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(self.__USER , self.__PASSWORD, self.__HOST, self.__PORT, dbname), 
                               echo = echo, paramstyle = "format")

        return engine
    
    
    
    
class Mongo:
    
    def __init__(self, mongo_host, mongo_user, mongo_password):
        self.MONGO_HOST = mongo_host
        self.MONGO_USER = mongo_user
        self.MONGO_PASSWORD = mongo_password
        self.conn = pymongo.MongoClient(self.MONGO_HOST, username = self.MONGO_USER, password = self.MONGO_PASSWORD)
        
    
    def connectDB(self, database_name, collection_name):
        return self.conn[database_name][collection_name]

    
    
    
    
    
    
    




