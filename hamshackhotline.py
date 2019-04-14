'''
File:    hamshackhotline.py
Author:  Ben Johnson AB3NJ
Purpose: Scrapes Hamshack Hotline's database pages to store users and bridges in a SqLite database
         Also provides functionality to update the database
'''


import os
import sqlite3
import requests
from bs4 import BeautifulSoup


USERS_URL = 'http://apps.wizworks.net:9091/results.php'


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

        if 'feature_table' not in kwargs:
            kwargs['feature_table'] = 'hh_features'

        self.args = kwargs
        print(self.args)

        self.start_database()


    def reset_database():
        pass


    def start_database(self):
        self.conn = sqlite3.connect(self.args['database'])
        self.c = self.conn.cursor()

        # makes tables if needed
        self.make_users()
        self.make_bridges()
        self.make_features()


    def make_users(self):
        self.c.execute(f'''CREATE TABLE IF NOT EXISTS {self.args['user_table']} (
                               record_number text,
                               callsign text,
                               name text,
                               city text,
                               state text,
                               country text,
                               network text,
                               number text,
                               ring_group text)''')
        
        self.conn.commit()


    def make_bridges(self):
        self.c.execute(f'''CREATE TABLE IF NOT EXISTS {self.args['bridge_table']} (
                               record_number text,
                               name text,
                               public text,
                               network text,
                               bridge number)''')

        self.conn.commit()


    def make_features(self):
        self.c.execute(f'''CREATE TABLE IF NOT EXISTS {self.args['feature_table']} (
                               record_number text,
                               name text,
                               public text,
                               network text,
                               bridge number)''')

        self.conn.commit()

    def drop_users(self):
        self.c.execute(f'''DROP TABLE {self.args['user_table']}''')
        self.conn.commit()

    def drop_bridges(self):
        self.c.execute(f'''DROP TABLE {self.args['bridge_table']}''')
        self.conn.commit()

    def drop_features(self):
        self.c.execute(f'''DROP TABLE {self.args['feature_table']}''')
        self.conn.commit()


    '''
    fetches the newest data from the website

    Note: Be nice to Hamshack Hotline here. Try to keep fetches low.
    '''
    def fetch_all(self):
        self.fetch_users()
        self.fetch_bridges()
        self.fetch_features()


    def fetch_users(self):
        # reset table
        self.drop_users()
        self.make_users()

        # request data
        page = requests.get(USERS_URL)
        
        # convert page
        soup = BeautifulSoup(page.content, features='lxml')

        # parse data
        e = []
        first = True
        for row in soup.find_all("tr"):
            # remove header row
            if first:
                first = False
                continue
            
            # actual data gathering
            for col in row.find_all("td"):
                if (len(col.contents) == 0):
                    e.append('')
                else:
                    e.append(col.contents[0])
            
            self.c.execute(f"""INSERT INTO
                                   {self.args['user_table']}
                               VALUES (
                                   '{e[0]}',
                                   '{e[1]}',
                                   '{e[2]}',
                                   '{e[3]}',
                                   '{e[4]}',
                                   '{e[5]}',
                                   '{e[6]}',
                                   '{e[7]}',
                                   '{e[8]}')""")
            e.clear()
        self.conn.commit()


    def fetch_bridges(self):
        pass


    def fetch_features(self):
        pass

    
    def clean_input(self, kwargs):
        for i in kwargs:
            kwargs[i] = kwargs[i].upper()


    def get_labels(self):
        labels = []
        labels_raw = self.c.description
        
        for l in labels_raw:
            labels.append(l[0])

        return labels


    def map_results(self):
        results = self.c.fetchall()
        labels = self.get_labels()
        mapped_results = []

        # error checking
        if len(results) == 0:
            print('here1')
            return None
        if len(labels) != len(results[0]):
            print('here2')
            return None

        for res in results:
            tmp = {}
            for i in range(0, len(results[0])):
                tmp[labels[i]] = res[i]
            mapped_results.append(tmp)

        return mapped_results


    '''
    Queries user table matching parameters from kwargs

    possible kwargs:
        callsign
        number
        name
    '''
    def query_users(self, **kwargs):
        self.clean_input(kwargs)
        mult_args = False
        query_args = []
        query = f"SELECT * FROM {self.args['user_table']}"

        # build string
        if len(kwargs) != 0:
            print('building string')
            if 'callsign' in kwargs:
                query = f"{query} WHERE callsign=?"
                query_args.append(kwargs['callsign'])
                mult_args = True
            if 'number' in kwargs:
                query = f"{query} {'AND ' if mult_args else ''}number=?"
                query_args.append(kwargs['number'])
                mult_args = True
            if 'name' in kwargs:
                query = f"{query} {'AND ' if mult_args else ''}name=?"
                query_args.append(kwargs['name'])
                mult_args = True

        print(query)
        print(tuple(query_args))

        self.c.execute(query, tuple(query_args))

        # objectify results and return
        return self.map_results()
