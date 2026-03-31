# Dockerfile
FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

# 1) 예전 guava 빼기 (있을 경우)
RUN if [ -f /opt/spark/jars/guava-14.0.1.jar ]; then \
      mv /opt/spark/jars/guava-14.0.1.jar /opt/spark/jars/guava-14.0.1.jar.bak ; \
    fi

# 2) 새 guava + failureaccess 추가
RUN curl -L -o /opt/spark/jars/guava-32.1.2-jre.jar \
      https://repo1.maven.org/maven2/com/google/guava/guava/32.1.2-jre/guava-32.1.2-jre.jar \
 && curl -L -o /opt/spark/jars/failureaccess-1.0.2.jar \
      https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.2/failureaccess-1.0.2.jar