import apache_beam as beam
import argparse
import logging
import os

from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from transformations.functions import ParseCSV, CollectDurationBy


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# setting options for dataflow  
parser = argparse.ArgumentParser()

parser.add_argument('--input',
                    dest='input',
                    default='gs://easy-beam-pipeline/demo_logs_10000.csv',
                    help='Input file to process.')

parser.add_argument('--output',
                    dest='output',
                    default='gs://easy-beam-pipeline/output',
                    help='Output file directory to write results to.')

known_args, pipeline_args = parser.parse_known_args()
options = PipelineOptions(pipeline_args)
options.view_as(SetupOptions).save_main_session = True


with beam.Pipeline(options=options) as p:

    processed = (
        p | 'read input file' >> ReadFromText(known_args.input)
          | 'skip header' >> beam.Filter(lambda row: 'name' not in row)
          | 'parse csv line' >> beam.ParDo(ParseCSV())
    )

    duration_by_country = (
        processed | 'collect tuple of (country_code, duration_stay)' >> beam.ParDo(CollectDurationBy(key_name='cc'))
        | 'group by country_code' >> beam.GroupByKey()
        | 'calculate average duration_stay' >> beam.CombineValues(beam.combiners.MeanCombineFn())
        | 'filter country with more 3 sec average duration_stay' >> beam.Filter(lambda tup: tup[1] >= 3.0)
        | 'convert country-grouped tuple to string' >> beam.Map(lambda tup: f'{tup[0]},{tup[1]}')
        | 'write out country data to csv' >> WriteToText(os.path.join(known_args.output, 'test_country_out.csv'))
    )

    duration_by_user = (
        processed | 'collect tuple of (name, duration_stay)' >> beam.ParDo(CollectDurationBy(key_name='name'))
        | 'group by user' >> beam.GroupByKey()
        | 'calculate total duration_stay' >> beam.CombineValues(sum)
        | 'filter user with more 5 sec duration_stay' >> beam.Filter(lambda tup: tup[1] >= 5.0)
        | 'convert user-grouped tuple to string' >> beam.Map(lambda tup: f'{tup[0]},{tup[1]}')
        |  'write out user data to csv' >> WriteToText(os.path.join(known_args.output, 'test_user_out.csv'))
    )
    
