FROM apache/hive:3.1.3

USER root
RUN rm -f /opt/hive/lib/postgresql-*.jar

ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/hive/lib/postgresql.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.2/hadoop-aws-3.1.2.jar /opt/hive/lib/hadoop-aws.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar /opt/hive/lib/aws-java-sdk-bundle.jar

COPY hive-site.xml /opt/hive/conf/hive-site.xml

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["bash", "/entrypoint.sh"]