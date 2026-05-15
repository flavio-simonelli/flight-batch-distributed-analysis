FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y wget unzip && rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/incubator/livy/0.8.0-incubating/apache-livy-0.8.0-incubating_2.12-bin.zip \
    && unzip -q apache-livy-0.8.0-incubating_2.12-bin.zip \
    && mv apache-livy-0.8.0-incubating_2.12-bin /opt/livy \
    && rm apache-livy-0.8.0-incubating_2.12-bin.zip

ENV SPARK_HOME=/opt/spark
ENV LIVY_HOME=/opt/livy
ENV PATH="${LIVY_HOME}/bin:${PATH}"

COPY conf/livy.conf ${LIVY_HOME}/conf/livy.conf

EXPOSE 8998

CMD ["sh", "-c", "/opt/livy/bin/livy-server start && tail -f /opt/livy/logs/livy--server.out"]
