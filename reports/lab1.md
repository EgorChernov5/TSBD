# Лабораторная работа №1 
Работу выполнили студенты Чебыкин Артём и Чернов Егор

## 1. Используемые данные
В качестве источника данных было выбрано официальное API мобильной игры **Clash of Clans**.  
Оно регулярно обновляется и содержит большое количество репрезентативных данных, что делает его удобным для построения ETL-процессов и сравнительного анализа различных систем хранения.

---

## 2. Используемые технологии
Для хранения данных были выбраны следующие сервисы:

- **Postgres** — реляционная СУБД.  
- **MongoDB** — нереляционная документо-ориентированная СУБД.  
- **Minio** — S3-совместимое объектное хранилище.

Такой выбор позволяет провести многогранное сравнение систем хранения.  

В качестве инструмента для организации ETL процессов используется **Apache Airflow**.  
Все сохраняемые данные имеют временное партиционирование, соответствующее времени их получения.

---

## 3. Устройство ETL-процессов

### 3.1. Реализация оберток для встроенных хуков Airflow
Для используемых систем хранения Airflow предоставляет встроенные хуки.  
Чтобы упростить работу, были реализованы собственные обертки, позволяющие выполнять необходимые операции в одну строку.

Ниже приведен пример подобной обертки:

```python
class AirflowAPIHook(BaseHook):
    def __init__(
            self, 
            conn_id: str = "airflow_api"
    ):
        super().__init__()
        self.conn_id = conn_id
        self.conn = self.get_connection(self.conn_id)
        self.base = f"{self.conn.conn_type or 'http'}://{self.conn.host}:{self.conn.port}"

    def get_token(self):
        url = self.base.rstrip("/") + "/auth/token"
        auth_payload = {
            "username": self.conn.login,
            "password": self.conn.password
        }

        r = requests.post(url, json=auth_payload, timeout=30)
        r.raise_for_status()

        return r.json()["access_token"]

    def get_task_instances(
        self,
        start_date: datetime
    ):
        token = self.get_token()
        url = (
            self.base.rstrip("/") +
            f"/api/v2/dags/~/dagRuns/~/taskInstances"
        )
        params = {"start_date_gt": start_date}
        headers = {"Authorization": f"Bearer {token}"}

        r = requests.get(url, params=params, headers=headers, timeout=120)
        r.raise_for_status()

        data = r.json()
        items = [
            item for item in data.get("task_instances") 
            if item["task_id"] != "fetch_metrics" 
            and item["end_date"] is not None
        ]

        if not items:
            logging.info("No task instances in last 7 days!")
            return []
        
        return items
````

---

### 3.2. Загрузка и чтение данных из выбранных БД

ETL-процесс разделён на две части:

1. **Загрузка сырых данных в хранилища.**
2. **Чтение данных и выполнение запроса «самый титулованный игрок в каждом клане».**

Пример реализации DAG:

```python
with DAG(
    dag_id="coc_minio_preprocess_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule="0 */6 * * *",
    catchup=False,
) as dag:

    get_raw_data_task = PythonOperator(
        task_id="get_raw_data",
        python_callable=get_raw_data
    )

    preprocess_raw_data_task = PythonOperator(
        task_id="preprocess_raw_data",
        python_callable=preprocess_raw_data
    )

    save_minio_raw_data_task = PythonOperator(
        task_id="save_minio_raw_data",
        python_callable=save_minio_raw_data
    )

    trigger_postprocess_data_DAG_task = TriggerDagRunOperator(
        task_id='trigger_postprocess_data_DAG',
        trigger_dag_id='coc_minio_postprocess_data',
        wait_for_completion=False
    )

    get_raw_data_task >> preprocess_raw_data_task >> \
    save_minio_raw_data_task >> trigger_postprocess_data_DAG_task
```

Второй DAG:

```python
with DAG(
    dag_id="coc_minio_postprocess_data",
    start_date=timezone.datetime(2025, 12, 9, 0, 0, 0),
    schedule=None,
    catchup=False,
) as dag:

    load_minio_raw_data_task = PythonOperator(
        task_id="load_minio_raw_data",
        python_callable=load_minio_raw_data
    )

    postprocess_minio_raw_data_task = PythonOperator(
        task_id="postprocess_minio_raw_data",
        python_callable=postprocess_minio_raw_data
    )

    load_minio_raw_data_task >> postprocess_minio_raw_data_task
```

---

### 3.3. Загрузка статики (иконок)

Был реализован скраппер, который парсит страницу
`https://clashofclans.fandom.com/wiki/Clash_of_Clans_Wiki`
и сохраняет все иконки персонажей в Minio. Это пригодится в будущих лабораторных работах.

---

### 3.4. Подсчёт метрик

Для подсчёта метрик скорости выполнения задач был создан отдельный DAG.
Он использует REST API Airflow для сбора статистики.

---

## 4. Качественное сравнение выбранных БД

| Критерий                     | Postgres | MongoDB | Minio |
| ---------------------------- | -------- | ------- | ----- |
| Размер данных (МБ)           | 2.18     | 5.55    | 6.80  |
| Скорость загрузки (сек)      | 1.880    | 1.730   | 3.515 |
| Скорость чтения (сек)        | 2.972    | 3.096   | 3.967 |
| Гарантии целостности (место) | 1        | 2       | 3     |
| Масштабируемость (место)     | 3        | 2       | 1     |

По результатам видно, что оптимальным решением является **Postgres**,
однако он плохо подходит для хранения изображений.

Поэтому выбрана комбинированная архитектура:

* **Minio** — для хранения необработанных данных и статики;
* **Postgres** — для хранения нормализованных табличных данных.