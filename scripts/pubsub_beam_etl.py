import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os

serviceaccount_file = 'shan-bigdata-6c83179d1393.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=serviceaccount_file

project_id = 'shan-bigdata'
subscription_id = 'projects/shan-bigdata/subscriptions/movie_topic-sub'
#output_topic = 'projects/shan-bigdata/subscriptions/comedy_topic'
output_topic = 'shan-bigdata:movie_sample.comedy_movie'
options =PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options = options)

output_schema = {'fields': [{'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
  {'name': 'movie_name', 'type': 'STRING', 'mode': 'NULLABLE'},
  {'name': 'movie_type', 'type': 'STRING', 'mode': 'NULLABLE'}]}

publish_pcollection = (
    p
    | "read from subscription: ">>beam.io.ReadFromPubSub(subscription = subscription_id)
    | "split each column" >>beam.Map(lambda rec: rec.decode('utf-8').split(','))
    | "filter the comedy movies" >>beam.Filter(lambda rec: 'Comedy' in rec[2])
    | "turn to dict" >>beam.Map(lambda rec: {'id':rec[0],'movie_name':rec[1],'movie_type':rec[2]})
    #| "combine row list into str" >>beam.Map(lambda rec: ' '.join(rec).encode('utf-8'))
    | "write to pubsub bigquery " >>beam.io.WriteToBigQuery(table = output_topic,schema=output_schema)
)

result = p.run()
result.wait_until_finish()