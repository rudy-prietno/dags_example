import datetime
import pendulum
from airflow.models import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook


sshHook = SSHHook(ssh_conn_id="ssh_connections")


"""
define component of dags
"""
local_tz = pendulum.timezone("Asia/Jakarta")

default_args ={
    'owner': 'data_engineers',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 9, tzinfo=local_tz),
    'email': ['11111@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay': datetime.timedelta(minutes=1),
    'params' : {
        'priority': 'P1'
    }
}

dag = DAG(
    'dags_sche_dacs',
    default_args=default_args,
    schedule_interval='0 */2 * * *',
    catchup=False
)

"""
define component state for batch process
"""

dac_stag = SSHOperator(
        task_id="sche_dac_stag",
        command= """
                 python3 /main.py
                 """,
        ssh_hook = sshHook,
        dag = dag
    )


dac_prod = SSHOperator(
        task_id="sche_dac_prod",
        command= """
                 python3 main.py
                 """,
        ssh_hook = sshHook,
        dag = dag
    )


"""
state
"""
dac_stag >> dac_prod
