FROM apache/airflow:2.10.5-python3.11

USER root

# test ssh connection
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc python3-dev openssh-client \
 && rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow SSH provider is required for SSHOperator
RUN pip install --no-cache-dir apache-airflow-providers-ssh==3.* openlineage-airflow
