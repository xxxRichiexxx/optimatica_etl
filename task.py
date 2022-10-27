import requests
import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import time
import datetime as dt


engine = sa.create_engine(
    f"""vertica+vertica_python://shveynikovab:{quote('s@vy7hSA')}@vs-da-vertica:5433/sttgaz"""
)

base_url ='https://gaz-marketing.ru/api/v2/'

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
        'new_name':[
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
        'new_name':[
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
            "Data.Data.DealerCity.Text",
            "Data.Data.Year.Text",
            "Data.Data.DateRange.Value.PeriodFrom",
            "Data.Data.DateRange.Value.PeriodTo",
            "Data.Data.Specialization.Text",
            "Data.Data.ZoneAM.Text",
            "Data.Data.RegionalSalesManager.Text",
            "Data.Data.TotalBudget.Value",
        ],
        'new_name':[
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
        'new_name':[
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
        "User": "integrator",
        "Password": "EYjAe9qmnKy7"
    }

    response = requests.post(url, verify=False, json=data)
    response.raise_for_status()

    try:
        return response.json()["token"]
    except:
        raise Exception('API не вернуло токен.')


def write_data(data, table, period_from, period_to):
    
    initial_data_volume_in_dwh = pd.read_sql_query(
        f"""
        SELECT COUNT(*) FROM sttgaz.{table}
        """,
        engine
    ).values[0][0]

    print('Обеспечение идемпотентности')

    pd.read_sql_query(
        f"""
        DELETE FROM sttgaz.{table}
        WHERE "CreatedAt" >= '{period_from}' AND "CreatedAt" <= '{period_to}'
        """,
        engine
    )

    print('Запись данных')

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
        WHERE "CreatedAt" >= '{period_from}' AND "CreatedAt" <= '{period_to}'
        """,
        engine
    ).values[0][0]

    if data_volume_from_api != recorded_data_volume:
        raise Exception(
            f"""Количество полученных из API данных не совпадает 
            с количеством записанных данных: {data_volume_from_api} != {recorded_data_volume}"""
        )

    final_data_volume_in_dwh = pd.read_sql_query(
        f"""
        SELECT COUNT(*) FROM sttgaz.{table}
        """,
        engine
    ).values[0][0]

    if final_data_volume_in_dwh < initial_data_volume_in_dwh:
        raise Exception(
            'Количество данных в dwh уменьшилось!'
        )        
    print(f'Получено данных: {data_volume_from_api}, записано данных: {recorded_data_volume}')
    print(f'Было данных: {initial_data_volume_in_dwh}, стало данных: {final_data_volume_in_dwh}')


def get_data(period_from, period_to, data_type):

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

    response = requests.post(url, headers=headers, verify=False, json=data)
    response.raise_for_status()

    data = pd.json_normalize(response.json())

    data = data[data_fields[data_type]['old_name']]

    data.columns = data_fields[data_type]['new_name']

    data['ts'] = dt.datetime.now()

    if data_type == 'YearPlanItem':
        data['Plan_Id'] = data['Plan_Id'].apply(lambda value: value[0]['Id'])
    elif data_type == 'QuarterPlanItem':
        data['QuarterPlan_Id'] = data['QuarterPlan_Id'].apply(lambda value: value[0]['Id'])

    print(data)

    write_data(data, f'stage_optimatica_{data_type}', period_from, period_to)


start_time = time.time()

# get_data("2022-01-01", "2022-12-31", 'YearPlan')
# get_data("2022-01-01", "2022-12-31", 'QuarterPlan')
# get_data("2022-01-01", "2022-12-31", 'MinimumBudget')
# get_data("2022-01-01", "2022-12-31", 'YearPlanItem')
# get_data("2022-01-01", "2022-12-31", 'QuarterPlanItem')
# get_data("2022-01-01", "2022-12-31", 'Placement')
# print(dt.datetime(2000,1,1).date())
period_from = dt.datetime.today() - dt.timedelta(days=365)
period_to = dt.datetime.today()
print(period_from, period_to)
print('Вренмя выполнения', time.time() - start_time)