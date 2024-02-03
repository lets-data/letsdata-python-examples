FROM public.ecr.aws/lambda/python:3.9

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip --no-cache-dir install -r requirements.txt

# install java
RUN rpm --import https://yum.corretto.aws/corretto.key 
RUN curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
RUN yum install -y java-1.8.0-amazon-corretto-devel.x86_64
RUN yum install -y tar
RUN yum install -y gzip
RUN yum install -y procps-ng

ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0-amazon-corretto/jre"
ENV PATH="/usr/lib/jvm/java/bin:${PATH}"

# versions of the spark / hadoop stack that we'd be using
ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_VERSION=1.12.634
ARG GOOGLE_GUAVA_VERSION=33.0.0-jre

# remove older jars in pyspark that we'd copy with versions that are compatible with our hadoop / aws installation
RUN rm /var/lang/lib/python3.9/site-packages/pyspark/jars/guava-14.0.1.jar 
RUN rm /var/lang/lib/python3.9/site-packages/pyspark/jars/hadoop-client-api-3.3.4.jar 
RUN rm /var/lang/lib/python3.9/site-packages/pyspark/jars/hadoop-client-runtime-3.3.4.jar

# copy with versions that are compatible with our hadoop / aws installation
RUN curl -o  /var/lang/lib/python3.9/site-packages/pyspark/jars/hadoop-aws-${HADOOP_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar  && \ 
    curl -o /var/lang/lib/python3.9/site-packages/pyspark/jars/hadoop-client-api-${HADOOP_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar && \ 
    curl -o /var/lang/lib/python3.9/site-packages/pyspark/jars/hadoop-client-runtime-${HADOOP_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar && \ 
    curl -o /var/lang/lib/python3.9/site-packages/pyspark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -o /var/lang/lib/python3.9/site-packages/pyspark/jars/guava-${GOOGLE_GUAVA_VERSION}.jar https://repo1.maven.org/maven2/com/google/guava/guava/${GOOGLE_GUAVA_VERSION}/guava-${GOOGLE_GUAVA_VERSION}.jar 

# Copy function code
COPY letsdata_lambda_function.py ${LAMBDA_TASK_ROOT}

RUN mkdir -p ${LAMBDA_TASK_ROOT}/letsdata_interfaces
COPY letsdata_interfaces/ ${LAMBDA_TASK_ROOT}/letsdata_interfaces/

RUN mkdir -p ${LAMBDA_TASK_ROOT}/letsdata_service
COPY letsdata_service/ ${LAMBDA_TASK_ROOT}/letsdata_service/

RUN mkdir -p ${LAMBDA_TASK_ROOT}/letsdata_utils
COPY letsdata_utils/ ${LAMBDA_TASK_ROOT}/letsdata_utils/

ENV SPARK_HOME="/var/lang/lib/python3.9/site-packages/pyspark"
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# create spark-env.sh
RUN mkdir $SPARK_HOME/conf
RUN echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh
RUN echo "JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto/jre" >> $SPARK_HOME/conf/spark-env.sh

#update spark-class
RUN echo "#!/usr/bin/env bash" > $SPARK_HOME/bin/spark-class
RUN echo "exec $JAVA_HOME/bin/java -cp /var/lang/lib/python3.9/site-packages/pyspark/conf/:/var/lang/lib/python3.9/site-packages/pyspark/jars/* -Xmx1g \"\$@\"" >> $SPARK_HOME/bin/spark-class

RUN mkdir /tmp/spark
RUN mkdir /tmp/spark/log
RUN mkdir /tmp/spark/work
RUN mkdir /tmp/spark/local
RUN mkdir /tmp/spark/pid

ENV SPARK_LOG_DIR="/tmp/spark/log"
ENV SPARK_WORKER_DIR="/tmp/spark/work"
ENV SPARK_LOCAL_DIRS="/tmp/spark/local"
ENV SPARK_PID_DIR="/tmp/spark/pid"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "letsdata_lambda_function.lambda_handler" ]
