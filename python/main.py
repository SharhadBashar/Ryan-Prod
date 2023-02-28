import pandas as pd
from tqdm import tqdm

from database import Database
from constants import LFM_COUNTRY_CODE 

db = Database(env = 'prod')

'''
data_gender = {
    'ContentFormatId': 342,
    'CountryCode': 'US',
    'IsMale': False,
    'Percentage': 12.3
}
data_age = {
    'ContentFormatId': 342,
    'CountryCode': 'US',
    'MinAge': 'Null',
    'MaxAge': 24,
    'Percentage': 12.3
}
'''

def write_gender(code):
    df = pd.read_csv('../data/gender/gender_{}.csv'.format(code))
    data = df.to_dict('records')
    for row in tqdm(data):
        if (code != 'LFM'):
            db.write_gender(row)
        elif (row['CountryCode'] in LFM_COUNTRY_CODE):
            db.write_gender(row)
    print('Done writing gender data for', code)

def write_age(code):
    df = pd.read_csv('../data/age/age_{}.csv'.format(code))
    data = df.to_dict('records')
    for row in tqdm(data):
        if (row['MinAge'] == 0): row['MinAge'] = 'Null'
        if (row['MaxAge'] == 100): row['MaxAge'] = 'Null'
        if (code != 'LFM'):
            db.write_age(row)
        elif (row['CountryCode'] in LFM_COUNTRY_CODE):
            db.write_age(row)
    print('Done writing age data for', code)

if __name__ == '__main__':
    write_age('NL')
    write_age('UK')
    write_age('LFM')

    write_gender('NL')
    write_gender('UK')
    write_gender('LFM')
