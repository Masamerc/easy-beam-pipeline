import apache_beam as beam
from apache_beam.io import ReadFromText

# user-defined transformations and functions
from transformations.ptransforms import ReadFromCsv
from transformations.functions import ConvertLog


with beam.Pipeline() as p:
    (
        p | "read input file" >> ReadFromCsv(["demo_input_data/demo_logs_*.csv"])
          | beam.ParDo(ConvertLog())
          | beam.Map(print)
    )
