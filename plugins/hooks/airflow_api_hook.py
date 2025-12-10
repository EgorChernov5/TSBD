from datetime import datetime
import logging

from airflow.sdk.bases.hook import BaseHook
import requests


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
        # Get token
        token = self.get_token()

        # Build request
        url = (
            self.base.rstrip("/") +
            f"/api/v2/dags/~/dagRuns/~/taskInstances"
        )
        params = {"start_date_gt": start_date}
        headers = {"Authorization": f"Bearer {token}"}

        # Get response
        r = requests.get(url, params=params, headers=headers, timeout=120)
        r.raise_for_status()

        data = r.json()
        items = [item for item in data.get("task_instances") 
                 if item["task_id"] != "fetch_metrics" and item["end_date"] is not None]

        if not items:
            logging.info("No task instances in last 7 days!")
            return []
        
        return items