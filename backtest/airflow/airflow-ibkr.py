# -*- coding: UTF-8 -*-
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay
from datetime import datetime, timedelta
import pytz
import logging


logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'backtrader_market_hours',
    default_args=default_args,
    description='Run Backtrader during US market hours',
    schedule_interval='@daily',  
    start_date=datetime(2024, 10, 29),
    tags=["ibkr", "backtrader"],
) as dag:
    
    def start_backtrader():
        logger.info("Backtrader starting... ")
    
    def stop_backtrader():
        logger.info("Backtrader stopping...")
         
    branch_workday = BranchDayOfWeekOperator(
        task_id="make_choice",
        follow_task_ids_if_true="branch_true",
        follow_task_ids_if_false="branch_false",
        week_day={WeekDay.MONDAY, WeekDay.TUESDAY, WeekDay.WEDNESDAY, WeekDay.THURSDAY, WeekDay.FRIDAY},
    )

    start_task = PythonOperator(
        task_id='start_backtrader',
        python_callable=start_backtrader,
        execution_timeout=timedelta(hours=6),
        trigger_rule='all_done'
    )

    stop_task = PythonOperator(
        task_id='stop_backtrader',
        python_callable=stop_backtrader,
    )

    weekend_task = EmptyOperator(task_id="branch_weekend")
 
    branch_workday >> [start_task >> stop_task, weekend_task]