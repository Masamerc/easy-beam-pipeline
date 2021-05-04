import apache_beam as beam
import datetime

from typing import Dict

class ConvertLog(beam.DoFn):
    def process(self, elem: Dict):
        yield {
            'name': elem['name'],
            'id': elem['id'],
            'cc': elem['cc'],
            'duration_stay': float(elem['duration_stay']),
            'accessed': datetime.datetime.strptime(elem['accessed'], '%Y-%m-%d %H:%M:%S',)
        }
