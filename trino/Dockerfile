FROM trinodb/trino:440

COPY catalog /etc/trino/catalog

USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]