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
FILE_LENGTH = 10_000
CUR_DIR = os.getcwd()
TODAY = datetime.datetime.today()

# Util function for fake timstamp
def random_date(start: datetime.datetime):
      curr = start + datetime.timedelta(minutes=random.randrange(60),seconds=random.randrange(60))
      return curr

# Sample input data
with open(os.path.join(CUR_DIR, 'demo_input_data/countries.json'), 'r') as f:
    countries = json.load(f)
    country_codes = [country['code'] for country in countries]

with open(os.path.join(CUR_DIR, 'demo_input_data/first-names.json'), 'r') as f:
    names = json.load(f)


# Generate randomw data
with open(os.path.join(CUR_DIR, 'demo_data_{}.csv'.format(FILE_LENGTH)), 'w') as f:
    for i in range(FILE_LENGTH):
        f.write(','.join(
                (
                    random.choice(names),
                    random.choice(country_codes),
                    str(random.choice(DURATION_STAY)/10.0),
                    f"{TODAY.strftime('%Y-%m-%d %H:%M:%S')}"
                )
            ) + '\n'
        )
