import requests
import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.vertica_operator import VerticaOperator


api_con = BaseHook.get_connection('optimatica')
dwh_con = BaseHook.get_connection('vertica')

ps = quote(dwh_con.password)
engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)

base_url = api_con.host


data_fields = {
    'YearPlan': {
        'old_name': [
            "Id",
            "Number",
            "ObjectType.Code",
            "ObjectType.Name",
            "ObjectClass.Code",
            "ObjectClass.Name",
            "Status.CreatedAt",
            "Status.CreatedBy.Name",
            "Status.Deleted",
            "Status.Frozen",
            "Data.Data.Dealer.Text",
            "Data.Data.Dealer.Value",
            "Data.Data.DealerCity.Text",
            "Data.Data.DealerLegalAddress.Text",
            "Data.Data.Year.Text",
            "Data.Data.DateRange.Value.PeriodFrom",
            "Data.Data.DateRange.Value.PeriodTo",
            "Data.Data.Specialization.Text",
            "Data.Data.ZoneAM.Text",
            "Data.Data.RegionalSalesManager.Text",
            "Data.Data.MinimumBudget.Value",
            "Data.Data.PlanBudget.Value",
            "Data.Data.FactBudget.Value",
            "Data.Data.Budget.Value",
            "Data.Data.YearPlanItems.Text",
            "WorkFlow.State.Name",
            "WorkFlow.Deadline",
            "WorkFlow.Activity",
        ],
        'new_name': [
            "Id",
            "Number",
            "ObjectType_Code",
            "ObjectType_Name",
            "ObjectClass_Code",
            "ObjectClass_Name",
            "CreatedAt",
            "CreatedBy",
            "Deleted",
            "Frozen",
            "Dealer",
            "Dealer_Id",
            "DealerCity",
            "DealerLegalAddress",
            "Year",
            "PeriodFrom",
            "PeriodTo",
            "Specialization",
            "ZoneAM",
            "RegionalSalesManager",
            "MinimumBudget",
            "PlanBudget",
            "FactBudget",
            "Budget",
            "YearPlanItems",
            "State",
            "Deadline",
            "Activity",
        ],
    },
    'QuarterPlan': {
        'old_name': [
            "Id",
            "Number",
            "ObjectType.Code",
            "ObjectType.Name",
            "ObjectClass.Code",
            "ObjectClass.Name",
            "Status.CreatedAt",
            "Status.CreatedBy.Name",
            "Status.Deleted",
            "Status.Frozen",
            "Data.Data.Dealer.Text",
            "Data.Data.Dealer.Value",
            "Data.Data.DealerCity.Text",
            "Data.Data.Quarter.Text",
            "Data.Data.DateRange.Value.PeriodFrom",
            "Data.Data.DateRange.Value.PeriodTo",
            "Data.Data.Specialization.Text",
            "Data.Data.ZoneAM.Text",
            "Data.Data.RegionalSalesManager.Text",
            "Data.Data.PlanBudget.Value",
            "Data.Data.YearPlanQuarterBudget.Value",
            "Data.Data.QuarterPlanItems.Text",
            "WorkFlow.State.Name",
            "WorkFlow.Deadline",
            "WorkFlow.Activity",
        ],
        'new_name': [
            "Id",
            "Number",
            "ObjectType_Code",
            "ObjectType_Name",
            "ObjectClass_Code",
            "ObjectClass_Name",
            "CreatedAt",
            "CreatedBy",
            "Deleted",
            "Frozen",
            "Dealer",
            "Dealer_Id",
            "DealerCity",
            "Quarter",
            "PeriodFrom",
            "PeriodTo",
            "Specialization",
            "ZoneAM",
            "RegionalSalesManager",
            "PlanBudget",
            "QuarterBudget",
            "QuarterPlanItems",
            "State",
            "Deadline",
            "Activity",
        ],
    },
    'MinimumBudget': {
        'old_name': [
            "Id",
            "Number",
            "ObjectType.Code",
            "ObjectType.Name",
            "ObjectClass.Code",
            "ObjectClass.Name",
            "Status.CreatedAt",
            "Status.CreatedBy.Name",
            "Status.Deleted",
            "Status.Frozen",
            "Data.Data.Dealer.Text",
            "Data.Data.Dealer.Value",
            "Data.Data.DealerCity.Text",
            "Data.Data.Year.Text",
            "Data.Data.DateRange.Value.PeriodFrom",
            "Data.Data.DateRange.Value.PeriodTo",
            "Data.Data.Specialization.Text",
            "Data.Data.ZoneAM.Text",
            "Data.Data.RegionalSalesManager.Text",
            "Data.Data.TotalBudget.Value",
        ],
        'new_name': [
            "Id",
            "Number",
            "ObjectType_Code",
            "ObjectType_Name",
            "ObjectClass_Code",
            "ObjectClass_Name",
            "CreatedAt",
            "CreatedBy",
            "Deleted",
            "Frozen",
            "Dealer",
            "Dealer_Id",
            "DealerCity",
            "Year",
            "PeriodFrom",
            "PeriodTo",
            "Specialization",
            "ZoneAM",
            "RegionalSalesManager",
            "TotalBudget",
        ],
    },
    'YearPlanItem': {
        'old_name': [
            "Id",
            "Number",
            "ObjectType.Code",
            "ObjectType.Name",
            "ObjectClass.Code",
            "ObjectClass.Name",
            "Status.CreatedAt",
            "Status.CreatedBy.Name",
            "Status.Deleted",
            "Status.Frozen",
            "Data.Data.Month.Text",
            "Data.Data.DateRange.Value.PeriodFrom",
            "Data.Data.DateRange.Value.PeriodTo",
            "Data.Data.Media.Text",
            "Data.Data.Model.Text",
            "Data.Data.TotalPrice.Value",
            "Data.Data.YearPlan.Value"
        ],
        'new_name': [
            "Id",
            "Number",
            "ObjectType_Code",
            "ObjectType_Name",
            "ObjectClass_Code",
            "ObjectClass_Name",
            "CreatedAt",
            "CreatedBy",
            "Deleted",
            "Frozen",
            "Month",
            "PeriodFrom",
            "PeriodTo",
            "Media",
            "Model",
            "TotalPrice",
            "Plan_Id"
        ],
    },
    'QuarterPlanItem': {
        'old_name': [
            "Id",
            "Number",
            "ObjectType.Code",
            "ObjectType.Name",
            "ObjectClass.Code",
            "ObjectClass.Name",
            "Status.CreatedAt",
            "Status.CreatedBy.Name",
            "Status.Deleted",
            "Status.Frozen",
            "Data.Data.DateRange.Value.PeriodFrom",
            "Data.Data.DateRange.Value.PeriodTo",
            "Data.Data.Model.Text",
            "Data.Data.Media.Text",
            "Data.Data.Description.Text",
            "Data.Data.Price.Value",
            "Data.Data.QuarterPlanObject.Value",
        ],
        'new_name': [
            "Id",
            "Number",
            "ObjectType_Code",
            "ObjectType_Name",
            "ObjectClass_Code",
            "ObjectClass_Name",
            "CreatedAt",
            "CreatedBy",
            "Deleted",
            "Frozen",
            "PeriodFrom",
            "PeriodTo",
            "Model",
            "Media",
            "Description",
            "Price",
            "QuarterPlan_Id",
        ],
    },
    'Placement': {
        'old_name': [
            "Id",
            "Number",
            "ObjectType.Code",
            "ObjectType.Name",
            "ObjectClass.Code",
            "ObjectClass.Name",
            "Status.CreatedAt",
            "Status.CreatedBy.Name",
            "Status.Deleted",
            "Status.Frozen",
            "Data.Data.Dealer.Text",
            "Data.Data.Dealer.Value",
            "Data.Data.DateRange.Value.PeriodFrom",
            "Data.Data.DateRange.Value.PeriodTo",
            "Data.Data.Specialization.Text",
            "Data.Data.ZoneAM.Text",
            "Data.Data.RegionalSalesManager.Text",
            "Data.Data.Media.Text",
            "Data.Data.Model.Text",
            "Data.Data.Site.Text",
            "Data.Data.Description.Text",
            "Data.Data.PublishCount.Value",
            "Data.Data.MeasureUnit.Value",
            "Data.Data.QuarterPlanItemRef.Text",
            "Data.Data.Price.Value",
            "WorkFlow.State.Name",
            "WorkFlow.Deadline",
            "WorkFlow.Activity",
        ],
        'new_name': [
            "Id",
            "Number",
            "ObjectType_Code",
            "ObjectType_Name",
            "ObjectClass_Code",
            "ObjectClass_Name",
            "CreatedAt",
            "CreatedBy",
            "Deleted",
            "Frozen",
            "Dealer",
            "Dealer_Id",
            "PeriodFrom",
            "PeriodTo",
            "Specialization",
            "ZoneAM",
            "RegionalSalesManager",
            "Media",
            "Model",
            "Site",
            "Description",
            "PublishCount",
            "MeasureUnit",
            "QuarterPlanItemRef",
            "Price",
            "State",
            "Deadline",
            "Activity",
        ],
    },
}


def get_token():

    url = base_url + 'auth'

    data = {
        "User": api_con.login,
        "Password": api_con.password
    }

    response = requests.post(url, verify=False, json=data)
    response.raise_for_status()

    try:
        return response.json()["token"]
    except KeyError:
        raise Exception('API ???? ?????????????? ??????????.')


def write_data(data, table, period_from, period_to):

    initial_data_volume_in_dwh = pd.read_sql_query(
        f"""
        SELECT COUNT(*) FROM sttgaz.{table}
        """,
        engine
    ).values[0][0]

    print('?????????????????????? ??????????????????????????????')

    pd.read_sql_query(
        f"""
        DELETE FROM sttgaz.{table}
        WHERE (CreatedAt at time zone 'Europe/Moscow')::date >= '{period_from}'
            AND (CreatedAt at time zone 'Europe/Moscow')::date <= '{period_to}'
        """,
        engine
    )

    print('???????????? ????????????')

    data.to_sql(
        table,
        engine,
        schema='sttgaz',
        if_exists='append',
        index=False,
    )

    data_volume_from_api = len(data)
    recorded_data_volume = pd.read_sql_query(
        f"""
        SELECT COUNT(*) FROM sttgaz.{table}
        WHERE (CreatedAt at time zone 'Europe/Moscow')::date >= '{period_from}'
            AND (CreatedAt at time zone 'Europe/Moscow')::date <= '{period_to}'
        """,
        engine
    ).values[0][0]

    if data_volume_from_api != recorded_data_volume:
        raise Exception(
            f"""???????????????????? ???????????????????? ???? API ???????????? ???? ??????????????????
            ?? ?????????????????????? ???????????????????? ????????????:
            {data_volume_from_api} != {recorded_data_volume}"""
        )

    final_data_volume_in_dwh = pd.read_sql_query(
        f"""
        SELECT COUNT(*) FROM sttgaz.{table}
        """,
        engine
    ).values[0][0]

    if final_data_volume_in_dwh < initial_data_volume_in_dwh:
        raise Exception(
            '???????????????????? ???????????? ?? dwh ??????????????????????!'
        )
    print(f'???????????????? ????????????: {data_volume_from_api}, ???????????????? ????????????: {recorded_data_volume}')
    print(f'???????? ????????????: {initial_data_volume_in_dwh}, ?????????? ????????????: {final_data_volume_in_dwh}')


#-------------- callable function -----------------

def get_data(data_type, **context):

    ex_date = context['execution_date']
    period_from = f'{ex_date.year-1}-{ex_date.month}-{ex_date.day}'
    period_to = f'{ex_date.year}-{ex_date.month}-{ex_date.day}'

    url = base_url + 'business-objects/query'

    headers = {
        'Authorization': f'Bearer {get_token()}'
    }

    data = {
        "ObjectType": data_type,
        "FieldMatch": {
            "Status.CreatedAt": {
                    "PeriodFrom": period_from,
                    "PeriodTo": period_to
            }
        }
    }

    print('?????????????????? ???????????? ???? ????????????: ', period_from, period_to)
    response = requests.post(url, headers=headers, verify=False, json=data)
    response.raise_for_status()

    data = pd.json_normalize(response.json())

    data = data[data_fields[data_type]['old_name']]

    data.columns = data_fields[data_type]['new_name']

    data['ts'] = dt.datetime.now()

    if data_type == 'YearPlanItem':
        data['Plan_Id'] = data['Plan_Id'].apply(
            lambda value: value[0]['Id']
        )
    elif data_type == 'QuarterPlanItem':
        data['QuarterPlan_Id'] = data['QuarterPlan_Id'].apply(
            lambda value: value[0]['Id']
        )
    elif data_type in ('YearPlan', 'QuarterPlan', 'MinimumBudget', 'Placement'):
        data['Dealer_Id'] = data['Dealer_Id'].apply(
            lambda value: value[0]['Id']
        )

    print(data)

    write_data(data, f'stage_optimatica_{data_type}', period_from, period_to)


#-------------- DAG -----------------

default_args = {
    'owner': '?????????????????? ????????????',
    'email': ['shveynikovab@st.tech'],
    'retries': 4,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'Optimatica',
        default_args=default_args,
        description='?????????????????? ???????????? ???? Optimatica.',
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='????????????')

    with TaskGroup('????????????????_????????????_??_stage_????????') as data_to_stage:

        tasks = []
        data_types = (
            'YearPlan',
            'QuarterPlan',
            'MinimumBudget',
            'YearPlanItem',
            'QuarterPlanItem',
            'Placement',
        )
        for data in data_types:
            tasks.append(
                PythonOperator(
                    task_id=f'??????????????????_????????????_{data}',
                    python_callable=get_data,
                    op_kwargs={'data_type': data},
                )
            )
        
        tasks

    with TaskGroup('????????????????????????_????????_DDS') as data_to_dds:

        dds_optimatica_dealers = VerticaOperator(
            task_id='update_dds_optimatica_dealers',
            vertica_conn_id='vertica',
            sql='dds_optimatica_dealers.sql',
        )

        dds_optimatica_year_plans = VerticaOperator(
            task_id='update_dds_optimatica_year_plans',
            vertica_conn_id='vertica',
            sql='dds_optimatica_year_plans.sql',
        )

        dds_optimatica_year_plan_items = VerticaOperator(
            task_id='update_dds_optimatica_year_plan_items',
            vertica_conn_id='vertica',
            sql='dds_optimatica_year_plan_items.sql',
        )

        dds_optimatica_placements = VerticaOperator(
            task_id='update_dds_optimatica_placements',
            vertica_conn_id='vertica',
            sql='dds_optimatica_placements.sql',
        )

        dds_optimatica_calendar = VerticaOperator(
            task_id='update_dds_optimatica_calendar',
            vertica_conn_id='vertica',
            sql='dds_optimatica_calendar.sql',
        )

        dds_optimatica_dealers >> dds_optimatica_year_plans >> [
            dds_optimatica_year_plan_items, dds_optimatica_placements
        ] >> dds_optimatica_calendar

    with TaskGroup('????????????????????????_????????_dm') as data_to_dm:

        dm_optimatica_plan_fact = VerticaOperator(
            task_id='update_dm_optimatica_plan_fact',
            vertica_conn_id='vertica',
            sql='dm_optimatica_plan_fact.sql',
        )

        dm_optimatica_plan_fact_aggregate = VerticaOperator(
            task_id='update_dm_optimatica_plan_fact_aggregate',
            vertica_conn_id='vertica',
            sql='dm_optimatica_plan_fact_aggregate.sql',
        )

        [dm_optimatica_plan_fact, dm_optimatica_plan_fact_aggregate]

    with TaskGroup('????????????????_????????????') as data_check:

        check_1 = VerticaOperator(
            task_id='checking_for_accuracy_of_execution',
            vertica_conn_id='vertica',
            sql='checking_for_accuracy_of_execution.sql'
        )

        check_1

    end = DummyOperator(task_id='??????????')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_check >> end
