'''
File:    hamshackhotline.py
Author:  Ben Johnson AB3NJ
Purpose: Scrapes Hamshack Hotline's database pages to store users and bridges in a SqLite database
         Also provides functionality to update the database
'''


import os
import sqlite3


class HamshackHotline:
    '''
    init takes as many optional args as the user wants

    If you want to add these tables to your own SqLite database,
      just put your file through. If the table names interfere,
      just make them whatever you want
    '''
    def __init__(self, **kwargs):
        # handle kwargs
        if 'database' not in kwargs:
            kwargs['database'] = 'default.db'

        if 'user_table' not in kwargs:
            kwargs['user_table'] = 'hh_users'

        if 'bridge_table' not in kwargs:
            kwargs['bridge_table'] = 'hh_bridges'

        self.args = kwargs

        self.start_database()


    def reset_database():
        pass


    def start_database():
        self.conn - sqlite3.connect(self.database)
        self.c = self.conn.cursor()

        # TODO: make tables if they don't exist
