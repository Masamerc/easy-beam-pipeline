import argparse
import datetime
import json
import logging
import os
import random
import uuid


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Constants / Settings
DURATION_STAY = range(1, 60)
CUR_DIR = os.getcwd()
TODAY = datetime.datetime.today()


# Sample input data
with open(os.path.join(CUR_DIR, 'demo_input_data/countries.json'), 'r') as f:
    countries = json.load(f)
    country_codes = [country['code'] for country in countries]

with open(os.path.join(CUR_DIR, 'demo_input_data/first-names.json'), 'r') as f:
    names = json.load(f)


def random_date(start: datetime) -> datetime:
    '''utility function that returns a fake timestamp'''
    curr = start + datetime.timedelta(minutes=random.randrange(60),seconds=random.randrange(60))
    return curr


def write_to_csv(file_length: int, header: bool=False) -> None:
    '''write fake records to csv file'''
    
    with open(os.path.join(CUR_DIR, 'demo_input_data/demo_logs_{}.csv'.format(file_length)), 'w') as f:
        if header:
            f.write(','.join(('name','id','cc','duration_stay','accessed')) + '\n')

        for _ in range(file_length):
            f.write(','.join(
                    (
                        random.choice(names),
                        uuid.uuid4().hex,
                        random.choice(country_codes),
                        str(random.choice(DURATION_STAY)/10.0),
                        f"{random_date(TODAY).strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                ) + '\n'
            )
    logging.info("Generated {:,} records - Written to CSV.".format(file_length))


def write_to_json(file_length: int) -> None:
    '''write fake records to record-oriented json file'''
    data_to_dump = [
        {
            'name': random.choice(names),
            'id': uuid.uuid4().hex,
            'cc': random.choice(country_codes),
            'duration_stay': random.choice(DURATION_STAY)/10.0,
            'accessed': f"{random_date(TODAY).strftime('%Y-%m-%d %H:%M:%S')}"
        } for _ in range(file_length)
    ]
    
    with open(os.path.join(CUR_DIR, 'demo_input_data/demo_logs_{}.json'.format(file_length)), 'w') as f:
        json.dump(data_to_dump, f, indent=2)

    logging.info("Generated {:,} records - Written to JSON.".format(file_length))        


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('num_records', type=int, help='number of records to generate')
    parser.add_argument('--csv', help='write records to csv',
                        action='store_true')
    parser.add_argument('-H', '--header', help='add header to csv', action='store_true')
    parser.add_argument('--json', help='write records to json',
                        action='store_true')

    args = parser.parse_args()
    
    if args.csv:
        if args.header:
            write_to_csv(file_length=args.num_records, header=True)
        else:
            write_to_csv(file_length=args.num_records)

    if args.json:
        write_to_json(file_length=args.num_records)
    
    # if no file type is specified it defaults to writing in both formats
    if (not args.csv) and (not args.json):
        write_to_csv(file_length=args.num_records)
        write_to_json(file_length=args.num_records)
