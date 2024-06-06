FROM apache/airflow:2.5.1
USER root
RUN apt-get update \
  && apt-get install -y git libpq-dev python3 python3-pip \
  && apt-get install -y openjdk-11-jdk \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && apt-get install -y procps \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64
RUN export JAVA_HOME

COPY Dockerfiles/res/requirements.txt .
User airflow
RUN pip3 install -r requirements.txt
