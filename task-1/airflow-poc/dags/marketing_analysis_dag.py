import os
from datetime import datetime, timedelta
import random
import logging
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

def write_email_log(status, context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']

    # Используем стандартную директорию логов Airflow, потом заберем - иначе прав не хватает
    log_file_path = Path("/opt/airflow/logs/email_simulation.log")

    message = (
        f"==========================================================\n"
        f"[{datetime.now()}] !!! СИМУЛЯЦИЯ EMAIL ИЗ КОЛЛБЭКА !!!\n"
        f"Письмо отправлено на: marketing_alerts@example.com\n"
        f"DAG: {dag_id}\n"
        f"Запуск (Run ID): {run_id}\n"
        f"Статус пайплайна: {status}\n"
        f"==========================================================\n\n"
    )

    try:
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(message)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        # Если не запишет, мы увидим ошибку в логах таски
        logging.error(f"КРИТИЧЕСКАЯ ОШИБКА ЗАПИСИ: {str(e)}")


# Настройка политик по умолчанию (Retry и оповещения на Email)
default_args = {
    'owner': 'marketing_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['marketing_alerts@example.com'],
    'email_on_failure': True,       # email при ошибке
    'email_on_success': True,       # email при успехе
    'retries': 3,                   # 3 повтора при сбое
    'retry_delay': timedelta(seconds=30), # Интервал между повторами
}

# Симуляция отправки email (мок, пишет в файл)

def email_success_callback(context):
    write_email_log("УСПЕШНО ЗАВЕРШЕН", context)

def email_failure_callback(context):
    write_email_log("КРИТИЧЕСКАЯ ОШИБКА", context)


# Инициализация DAG
with DAG(
    'marketing_batch_processing_poc',
    default_args=default_args,
    description='DAG для пакетной обработки данных маркетинга (около 1 млн записей)',
    schedule_interval=None,         # запуск только вручную для демонстрации
    catchup=False,
    on_success_callback=email_success_callback, # Trigger на успех всего пайплайна
    on_failure_callback=email_failure_callback, # Trigger на ошибку всего пайплайна
) as dag:

    # Шаг 1: Чтение из источника данных (мок)
    def _extract_mock_data(ti):
        # Имитируем чтение из базы/файлов. Генерируем случайное число строк около 1 млн.
        records_count = random.randint(500000, 1500000)
        logging.info(f"Успешно прочитаны данные. Обнаружено записей: {records_count}")
        # Сохраняем результат в XCom, чтобы передать следующему шагу
        ti.xcom_push(key='records_count', value=records_count)

    extract_mock_data = PythonOperator(
        task_id='extract_mock_data',
        python_callable=_extract_mock_data,
    )

    # Шаг 2: Анализ данных и определение ветки
    def _analyze_and_branch(ti):
        records = ti.xcom_pull(task_ids='extract_mock_data', key='records_count')
        logging.info(f"Анализ объема данных. Получено на обработку: {records} строк.")

        # Условие ветвления - порог в 1 000 000 записей
        if records >= 1000000:
            logging.info("Объем >= 1 000 000. Направляем на тяжелую обработку (Spark/Cloud).")
            return 'process_large_batch'
        else:
            logging.info("Объем < 1 000 000. Направляем на легкую обработку (Локально).")
            return 'process_small_batch'

    analyze_and_branch = BranchPythonOperator(
        task_id='analyze_and_branch',
        python_callable=_analyze_and_branch,
    )

    # Шаги веток обработки (мок)
    def _process_large():
        logging.info("=== Запущена тяжелая обработка Spark для 1 000 000+ строк ===")

    process_large_batch = PythonOperator(
        task_id='process_large_batch',
        python_callable=_process_large,
    )

    def _process_small():
        logging.info("=== Запущена быстрая локальная обработка для малого объема строк ===")

    process_small_batch = PythonOperator(
        task_id='process_small_batch',
        python_callable=_process_small,
    )

    # Финальная точка для объединения веток
    def _join_payloads():
        logging.info("=== Все выбранные ветки успешно объединены, пайплайн завершен ===")

    join_payloads = PythonOperator(
        task_id='join_payloads',
        python_callable=_join_payloads,
        trigger_rule=TriggerRule.ONE_SUCCESS # Выполнять, когда завершилась любая из веток
    )

    # Построение пайплайна
    extract_mock_data >> analyze_and_branch >> [process_large_batch, process_small_batch] >> join_payloads
