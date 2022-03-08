import csv, time
from google.cloud import pubsub_v1
import os
project_id = 'shan-bigdata'
topic = 'projects/shan-bigdata/topics/movie_topic'
serviceaccount_key = 'shan-bigdata-6c83179d1393.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']= serviceaccount_key

input_file = 'content/movies.csv'
publisher = pubsub_v1.PublisherClient()
with open(input_file,'rb') as file:
    for rec in file:
        print('publishing in Topic')
        publisher.publish(topic,rec)
        time.sleep(1)
