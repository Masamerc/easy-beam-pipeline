import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# user-defined transformations and functions
from transformations.ptransforms import ReadFromCsv, WriteToCsv, GroupType
from transformations.functions import ConvertLog, CoolectDurationBy


CURDIR = os.getcwd()
INDIR = os.path.join(CURDIR, 'demo_input_data')
OUTDIR = os.path.join(CURDIR, 'demo_output_data')

options = PipelineOptions()
options.view_as(StandardOptions).runner = 'Direct'

with beam.Pipeline(options=options) as p:
    
    processed = (
        p | 'read input file' >> ReadFromCsv([os.path.join(INDIR, 'demo_logs_*.csv')])
          | 'converting data types' >> beam.ParDo(ConvertLog())
    )

    duration_by_country = (
        processed | 'collect tuple of (country_code, duration_stay)' >> beam.ParDo(CoolectDurationBy(key_name='cc'))
        | 'group by country_code' >> beam.GroupByKey()
        | 'calculate average duration_stay' >> beam.CombineValues(beam.combiners.MeanCombineFn())
        | 'filter country with more 3 sec average duration_stay' >> beam.Filter(lambda tup: tup[1] >= 3.0)
        | 'write country data to csv' >> WriteToCsv(os.path.join(OUTDIR, 'test_country.csv'), GroupType.COUNTRY)
    )

    duration_by_user = (
        processed | 'collect tuple of (name, duration_stay)' >> beam.ParDo(CoolectDurationBy(key_name='name'))
        | 'group by user' >> beam.GroupByKey()
        | 'calculate total duration_stay' >> beam.CombineValues(sum)
        | 'filter user with more 5 sec duration_stay' >> beam.Filter(lambda tup: tup[1] >= 5.0)
        | 'write user data to csv' >> WriteToCsv(os.path.join(OUTDIR, 'test_user.csv'), GroupType.USER)
    )

    
