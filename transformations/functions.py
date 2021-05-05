import apache_beam as beam
import datetime
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConvertLog(beam.DoFn):
    '''
    casts duration_stay to float and accessed to datetime object
    '''
    logger.info('converting logs')
    def process(self, elem: Dict):
        yield {
            'name': elem['name'],
            'id': elem['id'],
            'cc': elem['cc'],
            'duration_stay': float(elem['duration_stay']),
            'accessed': datetime.datetime.strptime(elem['accessed'], '%Y-%m-%d %H:%M:%S',)
        }


class CollectDurationBy(beam.DoFn):
    '''
    returns a tuple which consist of (key_name, duration_stay)
    '''
    def __init__(self, key_name:str):
        self.key_name = key_name

    def process(self, elem: Dict):
        yield (elem[self.key_name], elem['duration_stay'])


class ParseCSV(beam.DoFn):
    '''
    parse raw csv line to dictionary (only used for the dataflow script)
    '''
    def process(self, elem):
        name, id, cc, duration_stay, accessed = elem.split(',')
        yield {
            'name': name,
            'id': id,
            'cc': cc,
            'duration_stay': float(duration_stay),
            'accessed': datetime.datetime.strptime(accessed, '%Y-%m-%d %H:%M:%S',)
        }
