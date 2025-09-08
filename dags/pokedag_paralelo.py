from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import sys
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "include"))
from scripts.etl_lastfm import download_lastfm, preprocess, train_model, generate_recommendations

DEFAULT_URL = "http://files.grouplens.org/datasets/hetrec2011/hetrec2011-lastfm-2k.zip"

with DAG(
    dag_id="lastfm_recommender",
    start_date=datetime(2025, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
    default_args={
        "owner": "data-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["recommender","lastfm","music"],
) as dag:

    def _download():
        url = Variable.get("lastfm_url", DEFAULT_URL)
        return download_lastfm(url)

    t_download = PythonOperator(
        task_id="download_lastfm",
        python_callable=_download,
    )

    t_preprocess = PythonOperator(
        task_id="preprocess",
        python_callable=preprocess,
    )

    t_train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    def _gen():
        n = int(Variable.get("topn_per_user", 5))
        return generate_recommendations(n_per_user=n)

    t_generate = PythonOperator(
        task_id="generate_recommendations",
        python_callable=_gen,
    )

    t_download >> t_preprocess >> t_train >> t_generate
