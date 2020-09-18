from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from json import dumps


from kafka import KafkaProducer

import uuid

def getUsername(d):
  return d['from']['username']

def getText(d):
  return d['text']


def data_cleaner(x):
  try:
    d = json.loads(x)

    return {
      
      'username': getUsername(d),
      'text': getText(d)
     
    }
  except:
    return {
      #'username': None,
      #'text': None
    }

producer = KafkaProducer(bootstrap_servers=['kafkaServer:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def sendToKafka(record):
  producer.send('processed-data', record)
  producer.flush()

def handler(message):
  records = message.collect()
  for record in records:
    record = str(record)
    if(not 'None' in record):
      sendToKafka(record)

sc = SparkContext("local[2]", "DataCleaner")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 1)

zooKeeperAddress = "10.0.100.22:2181"
topic = "data"
kvs = KafkaUtils.createStream(ssc, zooKeeperAddress, "spark-processing", {topic: 1})
relevant_data = kvs.map(lambda x: data_cleaner(x[1]))

relevant_data.foreachRDD(handler)

relevant_data.pprint()

ssc.start()
ssc.awaitTermination()
