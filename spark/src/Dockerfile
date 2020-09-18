FROM openjdk:8-jre

ENV PATH $SPARK_DIR/bin:$PATH
ENV SPARK_VERSION=2.4.5
ENV SPARK_DIR=/opt/spark
ENV PATH $SPARK_DIR/bin:$PATH

ADD setup/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz /opt

RUN apt-get update && apt-get -y install bash python python-pip libatlas3-base libopenblas-base

RUN pip install pyspark numpy elasticsearch kafka-python telepot self
# Create Sym Link 
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop2.7 ${SPARK_DIR} 
ADD dataset /opt/tap/spark/dataset

ADD code/*  /opt/tap/
# Add Spark Manager
ADD spark-manager.sh $SPARK_DIR/bin/spark-manager

WORKDIR ${SPARK_DIR}
ENTRYPOINT [ "bin/spark-manager" ]


