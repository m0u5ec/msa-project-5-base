import os
import csv
import sys
from datetime import datetime
import psycopg2

def export_table():
    # Получаем настройки подключения из переменных окружения K8s
    db_host = os.getenv("DB_HOST", "postgres")
    db_name = os.getenv("DB_NAME", "analytics")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")

    current_date = datetime.now().strftime("%Y-%m-%d")
    output_filename = f"/tmp/shipments_{current_date}.csv"

    print(f"[{datetime.now()}] Старт выгрузки таблицы 'shipments'...")

    try:
        # Подключение к PostgreSQL
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()

        # Выполняем простой экспорт данных
        cursor.execute("SELECT id, client_id, driver_id, vehicle_id, status, created_at FROM shipments;")

        # Поточно пишем в CSV файл чанками по 2000 строк
        with open(output_filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Пишем заголовки столбцов
            writer.writerow([desc[0] for desc in cursor.description])

            while True:
                rows = cursor.fetchmany(2000)
                if not rows:
                    break
                writer.writerows(rows)

        cursor.close()
        conn.close()

        print(f"[{datetime.now()}] Успех! Файл сохранен: {output_filename}")
        print(f"Размер выгрузки: {os.path.getsize(output_filename)} байт.")

    except Exception as e:
        print(f"[{datetime.now()}] КРИТИЧЕСКАЯ ОШИБКА: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    export_table()
