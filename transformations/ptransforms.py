import apache_beam as beam
import glob

from csv import DictReader


class ReadFromCsv(beam.PTransform):
    def __init__(self, file_patterns):
        self.file_patterns = file_patterns

    @staticmethod
    def read_lines_from_csv(file_name):
        with open(file_name, 'r') as f:
            reader = DictReader(f)
            for row in reader:
                yield dict(row)

    def expand(self, pcollection):
        return (
            pcollection
            | 'create inputs from file patterns' >> beam.Create(self.file_patterns)
            | 'expand file patterns' >> beam.FlatMap(glob.glob)
            | 'read lines as dict for each found file' >> beam.FlatMap(self.read_lines_from_csv)
        )
