from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

# SSH
ssh_hook = SSHHook(
    remote_host="spark-master",
    username="sparkuser",
    password="sparkpass",
    port=22
)

SPARK_SUBMIT = "/spark/bin/spark-submit"
COMMON_CONF = [
    "--master", "spark://spark-master:7077",
    "--conf", "spark.executor.memory=1g",
    "--conf", "spark.executor.cores=1",
    "--conf", "spark.cores.max=4",
]

def make_spark_task(task_id: str, script_path: str) -> SSHOperator:
    """Reusable factory for Spark SSH tasks"""
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ssh_hook,
        command=" ".join([
            SPARK_SUBMIT,
            *COMMON_CONF,
            script_path,
            "--rundate", "{{ ds_nodash }}"
        ]),
    )
