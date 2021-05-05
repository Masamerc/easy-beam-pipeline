import apache_beam as beam
import datetime
import glob
import logging
import os

from csv import DictReader,DictWriter
from enum import Enum
from typing import Dict, List


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GroupType(Enum):
    '''
    switcher to indicate which attribute, user or country,
    the data is being grouped by.
    '''
    USER = 1
    COUNTRY = 2


class ReadFromCsv(beam.PTransform):
    '''
    read csv file into a list of dictionary entries
    NOTE: csv file must have a header row
    '''
    def __init__(self, file_patterns: List[str]):
        self.file_patterns = file_patterns

    @staticmethod
    def read_lines_from_csv(file_name):
        with open(file_name, 'r') as f:
            reader = DictReader(f)
            for row in reader:
                yield dict(row)

    def expand(self, pcollection):
        logger.info('reading csv files: {}'.format(self.file_patterns))
        return (
            pcollection
            | 'create inputs from file patterns' >> beam.Create(self.file_patterns)
            | 'expand file patterns' >> beam.FlatMap(glob.glob)
            | 'read lines as dict for each found file' >> beam.FlatMap(self.read_lines_from_csv)
        )


class WriteToCsv(beam.PTransform):
    def __init__(self, output_path: str, group_type: GroupType):
        file_name, ext = os.path.splitext(output_path)
        self.output_file = f"{file_name}_{str(datetime.datetime.timestamp(datetime.datetime.now()))}{ext}"

        if group_type == GroupType.USER:
            self.group_type = 'user'
        elif group_type == GroupType.COUNTRY:
            self.group_type = 'country'
        else:
            raise TypeError('group type should either be GroupType.USER or GroupType.COUNTRY')

        self.field_names = [self.group_type, 'value']
        self.handler = open(self.output_file, 'a')
        writer = DictWriter(self.handler, fieldnames=self.field_names)
        writer.writeheader()

    def __del__(self):
        self.handler.close()

    def write_dict_to_csv(self, row: Dict):
        writer = DictWriter(self.handler, fieldnames=self.field_names)
        writer.writerow(row)

    def expand(self, pcollection):
        logger.info('writing to csv files: {}'.format(self.output_file))
        return (
            pcollection
            | 'convert tuple to dict' >> beam.Map(lambda tup: {self.group_type: tup[0], 'value': tup[1]})
            | 'write data to csv file' >> beam.Map(self.write_dict_to_csv)
        )
        
    