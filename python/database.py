import json
import pickle
import pyodbc
import pandas as pd
from tqdm import tqdm

from constants import LFM_COUNTRY_CODE 

class Database:
    def __init__(self, env = 'staging'):
        with open('../config/database.json') as file:
            database_info = json.load(file)
        self.conn_common = self._database_conn(database_info[env], 'common')
        self.conn_dmp = self._database_conn(database_info[env], 'dmp')
        
    def _database_conn(self, database_info, database):
        return 'Driver={};\
                Server={};\
                Database={};\
                Trusted_Connection=yes'.format(
            database_info['driver'], database_info['server'], 
            database_info['database'][database]
        )
    
    def test_conn(self):
        conn = pyodbc.connect(self.conn_dmp)
        query = """SELECT
                        TOP (10) *
                   FROM
                        dbo.AgeRangeByContentFormatId  
                """
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor:
            print('Connection successful')
        else:
            print('Connection UNsuccessful')
        cursor.close()
        
    def write_gender(self, data):
        conn = pyodbc.connect(self.conn_dmp)
        query = """INSERT INTO 
                    dbo.GenderByContentFormatId (ContentFormatId, CountryCode, IsMale, Percentage, ContentFormatSourceId, GeoItemId)
                   VALUES 
                    ({}, '{}', '{}', {}, 3, Null)
                """.format(data['ContentFormatId'], data['CountryCode'], data['IsMale'], data['Percentage'])
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        
    def write_age(self, data):
        conn = pyodbc.connect(self.conn_dmp)
        query = """INSERT INTO 
                    dbo.AgeRangeByContentFormatId (ContentFormatId, CountryCode, MinAge, MaxAge, Percentage, ContentFormatSourceId, GeoItemId)
                   VALUES 
                    ({}, '{}', {}, {}, {}, 3, Null)
                """.format(data['ContentFormatId'], data['CountryCode'], data['MinAge'], data['MaxAge'], data['Percentage'])
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()

'''
data_gender = {
    'ContentFormatId': 342,
    'CountryCode': 'US',
    'IsMale': False,
    'Percentage': 12.3
}
'''

def write_gender(code):
    df = pd.read_csv('../data/gender/gender_{}.csv'.format(code))
    data = df.to_dict('records')
    for row in tqdm(data):
        if (code != 'LFM'):
#             db.write_gender(row)
            print(row)
        elif (row['CountryCode'] in LFM_COUNTRY_CODE):
#             db.write_gender(row)
            print(row)
    print('Done writing gender data for', code)

'''
data_age = {
    'ContentFormatId': 342,
    'CountryCode': 'US',
    'MinAge': 'Null',
    'MaxAge': 24,
    'Percentage': 12.3
}
'''

def write_age(code):
    df = pd.read_csv('../data/age/age_{}.csv'.format(code))
    data = df.to_dict('records')
    for row in tqdm(data):
        if (row['MinAge'] == 0): row['MinAge'] = 'Null'
        if (row['MaxAge'] == 100): row['MaxAge'] = 'Null'
        if (code != 'LFM'):
#             db.write_age(row)
            print(row)
        elif (row['CountryCode'] in LFM_COUNTRY_CODE):
#             db.write_age(row)
            print(row)
    print('Done writing age data for', code)

if __name__ == '__main__':
    db = Database(env = 'prod')
    db.test_conn()