from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
import random
import time

# Функція для примусового встановлення статусу DAG як успішного
def mark_dag_success(ti, **kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Назва з'єднання з базою даних MySQL
CONN = "goit_mysql_db_cryptophobic"
SCHEMA = "olympic_cryptophobic"
TABLE = "athlete_event_results"

DELAY_SECONDS = 5
FRESH_LIMIT = 30

def choose_medal_func():
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Обрано медаль: {medal}")
    return {"Bronze": "bronze_task", "Silver": "silver_task"}.get(medal, "gold_task")

def delay_func():
    print(f"Затримка на {DELAY_SECONDS} секунд...")
    time.sleep(DELAY_SECONDS)

def generate_insert_sql(medal: str) -> str:
    return f"""
    INSERT INTO {SCHEMA}.{TABLE} (medal_type, count, created_at)
    SELECT '{medal}', COUNT(*), NOW()
    FROM olympic_dataset.athlete_event_results
    WHERE medal = '{medal}';
    """

# Визначення DAG
with DAG(
        'DAG_Cryptophobic',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["cryptophobic"]  # Теги для класифікації DAG
) as dag:
    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = MySqlOperator(
        task_id="create_schema",
        mysql_conn_id=CONN,
        sql=f"CREATE DATABASE IF NOT EXISTS {SCHEMA};",
    )

    # Завдання для створення таблиці (якщо не існує)
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONN,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(16) NOT NULL,
            count INT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    )

    choose_medal = BranchPythonOperator(
        task_id="choose_medal",
        python_callable=choose_medal_func,
    )

    bronze_task = MySqlOperator(
        task_id="bronze_task",
        mysql_conn_id=CONN,
        sql=generate_insert_sql("Bronze"),
    )
    silver_task = MySqlOperator(
        task_id="silver_task",
        mysql_conn_id=CONN,
        sql=generate_insert_sql("Silver"),
    )
    gold_task = MySqlOperator(
        task_id="gold_task",
        mysql_conn_id=CONN,
        sql=generate_insert_sql("Gold"),
    )

    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_func,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_recent_record = SqlSensor(
        task_id="check_recent_record",
        conn_id=CONN,
        sql=f"""
        SELECT
          CASE
            WHEN TIMESTAMPDIFF(
                   SECOND, COALESCE(MAX(created_at), '1970-01-01'), NOW()
                 ) <= {FRESH_LIMIT}
            THEN 1 ELSE 0 END AS is_recent
        FROM {SCHEMA}.{TABLE};
        """,
        mode="reschedule",
        poke_interval=5,
        timeout=60,
    )

    create_schema >> create_table >> choose_medal >> [bronze_task, silver_task, gold_task]
    [bronze_task, silver_task, gold_task] >> delay_task >> check_recent_record