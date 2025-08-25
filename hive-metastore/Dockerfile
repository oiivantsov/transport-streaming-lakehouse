FROM apache/hive:3.1.3

USER root
RUN rm -f /opt/hive/lib/postgresql-*.jar

ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/hive/lib/postgresql.jar

COPY hive-site.xml /opt/hive/conf/hive-site.xml

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["bash", "/entrypoint.sh"]

