# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, SparkSession
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.ml.linalg import VectorUDT, SparseVector,DenseVector
import json
from elasticsearch import Elasticsearch
import uuid




index=0


# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "DataPrediction")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint")

# Elasticsearch data
elastic_host = "10.0.100.51"
elastic_index = "sentimenti"
elastic_document = "_doc"
elasticSearch = Elasticsearch(hosts=[elastic_host])

# Create a DStream that will connect to Kafka
zooKeeperAddress = "10.0.100.22:2181"
topic = "processed-data"
lines =  KafkaUtils.createStream(ssc, zooKeeperAddress, "spark-mllib", {topic: 1})

def clearData(x):
    return x[0][1].replace("'",'"').replace('u"','"')[1:-1]

# Count each word in each batch
pairs = lines.map(lambda word: (word, 1))
windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60*5, 30) \
                          .filter(lambda x: True if x[1] != 0 else False) \
                          .map(clearData)

def jsonToDataFrame(records):
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    return spark.read.json(sc.parallelize(records))

#Time functions
def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def unix_time_millis(dt):
    return int(unix_time(dt) * 1000)

def utcToInt(str):
    a = datetime.strptime(str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return long(unix_time_millis(a))

def utcNowPlusN(n):
    return str(datetime.utcnow() + timedelta(minutes = n)).replace(' ', 'T') + 'Z'

def createInputUtc(utc):
    spark = SparkSession.builder.getOrCreate()
    int_utc = (utcToInt(utc))
    print "timestamp: ", utc, "num_time: ", int_utc

    schema = T.StructType([T.StructField('features', VectorUDT())])
    return spark.createDataFrame([Row(features=DenseVector([int_utc]))], schema = schema)
###############

def getItemFromDataFrame(dataframe, column, index):
  return dataframe.select(column).collect()[index][column]


def polarityScore(stringa):
    emoticonsPositive = ('😇', '😊', '❤️', '😘', '💞', '💖', '🤗', '💕', '👏', '🎉', '👍', '🔝')
    emoticonsNegative = ('😂', '😡', '😠', '😭', '🤦‍', '🤷🏼‍', '😞', '😱', '😓', '👎')
    radiciPositive = ("ama", "amo", "affett", "allegr", "amabil", "apprezz", "armon", "affet",
                      "applaus", "abbracc", "ador",
                      "bell", "ben", "beat", "brav", "buon", "benef", "brill",
                      "cuor", "coeren", "celebr",
                      "dolc", "divert",
                      "evviva", "emoz", "elog",
                      "felic", "fest", "facil",
                      "gentil", "god", "grazi", "generos", "gioi",
                      "innamor", "interes", "insieme",
                      "libert",
                      "maestos", "miglior",
                      "pace", "passion", "perfe", "piac", "pura", "purezz", "prezios", "promuov",
                      "rilass", "riabbracc",
                      "solida", "spero", "speran", "success", "sì", "sacr", "stupend", "spettacol",
                      "viv", "vin", "valor", "vale", "vera", "vittor")
    radiciNegative = ("accus", "amaro", "amarez", "arm", "ammazz",
                      "brut", "boicott", "boh", "bho",
                      "condann", "cazz", "crisi", "critic", "coglion",
                      "decent", "depress", "detest", "disgr", "delir", "damn", "drog",
                      "fumo", "fuma",
                      "esorcis",
                      "fascis",
                      "guai",
                      "immat", "insult", "inuman", "impone",
                      "lent",
                      "mor", "merd", "male", "maial",
                      "no", "nega", "ncazz", "negr",
                      "od", "oscur",
                      "perde", "preoccup", "pusillanim", "porc",
                      "rovina",
                      "schif", "satan", "sprof", "soffri", "soffer", "scandal", "scars", "sporc", "spar", "stalk",
                      "trist", "trash", "tarocc",
                      "vergogn",
                      "zitt")
    radiciDaEscludere = ("now", "nom", "not", "noz", "amp", "nor", "veramente", "imponent")

    val = 0
    list = stringa.split()
    print("dataframe splittato",list)
    for word in list:

        if word in emoticonsPositive:
            return 2  # messaggio considerato positivo

        elif word in emoticonsNegative:
            return 0  # messaggio considerato negativo

        else:
            if not word.startswith(radiciDaEscludere):

                if word.startswith(radiciPositive):
                    val = val + 1

                if word.startswith(radiciNegative):
                        val = val - 1

                # Il 'non' cambia dinamicamente la polarita' del msg
                if word == "non":
                    val = val * -1

                # Tutte le parole prima di 'ma' e 'però' non vengono considerate
                if word in ("ma", "però"):
                    val = 0
    print('valutazione')
    if val > 0:
        label = 2
    elif val < 0:
        label = 0
    else:
        label = 1
    print('invio label')
    return label

def sendToElasticSearch(record):
    uuId = uuid.uuid4()
    print(uuId)
    try:
        print("invio in corso")
        response = Elasticsearch(hosts=[elastic_host]).index(
            index = elastic_index,
            doc_type = elastic_document,
            id = uuId,
            body = record,
        )
    except Exception as e:
        print(e)

def handler(message):
    records = message.collect()
    if(records != []):
        try:
            df = jsonToDataFrame(records)
            df = df.dropna()
            df.show(df.count(), truncate=False)
            str_plus_n = utcNowPlusN(0)
            records = list(filter(None, records))
            recordsJSON = json.loads(records[1])
            print("il record",records[0],"altro: ",records[1])
            sentiment = polarityScore(str(recordsJSON['text']))
            print("sentimento scoperto: ",sentiment)
            print("creazione response")
            response = {
                'type': 'predicted_sentiment',
                'timestamp': str_plus_n,
                'username': str(recordsJSON['username']),
                'text': str(recordsJSON['text']),
                'sentimento': sentiment
            }
            print('response: ',response)
            sendToElasticSearch(response)
            records.pop(1)
        except Exception as e:
            print(e)

windowedWordCounts.foreachRDD(handler)

# Print the first ten elements of each RDD generated in this DStream to the console
windowedWordCounts.pprint()

ssc.start()
ssc.awaitTermination()