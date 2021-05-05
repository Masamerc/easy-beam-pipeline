import apache_beam as beam
import datetime

from typing import Dict

class ConvertLog(beam.DoFn):
    '''
    casts duration_stay to float and accessed to datetime object
    '''
    def process(self, elem: Dict):
        yield {
            'name': elem['name'],
            'id': elem['id'],
            'cc': elem['cc'],
            'duration_stay': float(elem['duration_stay']),
            'accessed': datetime.datetime.strptime(elem['accessed'], '%Y-%m-%d %H:%M:%S',)
        }


class CoolectDurationBy(beam.DoFn):
    '''
    returns a tuple which consist of (key_name, duration_stay)
    '''
    def __init__(self, key_name:str):
        self.key_name = key_name

    def process(self, elem: Dict):
        yield (elem[self.key_name], elem['duration_stay'])
