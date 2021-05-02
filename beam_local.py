import apache_beam as beam
from apache_beam.io import ReadFromText

with beam.Pipeline() as p:
    (
        p | "read imput file" >> ReadFromText("demo_data_10000.csv")
          | beam.Map(print)
    )
