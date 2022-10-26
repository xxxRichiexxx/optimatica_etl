import requests
import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import time
import datetime as dt
from pprint import pprint 


engine = sa.create_engine(
    f"""vertica+vertica_python://shveynikovab:{quote('s@vy7hSA')}@vs-da-vertica:5433/sttgaz"""
)


base_url ='https://gaz.optimatica.ru/api/v2/'





def get_token():

    url = base_url + 'auth'

    data = {
        "User": "integrator",
        "Password": "2ZqpRmjGms"
    }

    response = requests.post(url, verify=False, json=data)
    response.raise_for_status()

    try:
        return response.json()["token"]
    except:
        raise Exception('API не вернуло токен.')



def write_data(data, table, id_field):

    print('Обеспечение идемпотентности')

    ids_for_del = tuple(set(data[id_field].values))

    if len(ids_for_del) > 1:
        query_tail = f'IN {ids_for_del}'
    else:
        query_tail = f' = {ids_for_del[0]}'

    pd.read_sql_query(
        f"""
        DELETE FROM sttgaz.{table}
        WHERE "{id_field}" 
        """ + query_tail,
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


def get_budgets(PeriodFrom, PeriodTo):

    url = base_url + 'business-objects/query'

    headers = {
        'Authorization': f'Bearer {get_token()}'
    }

    data = {
        "FieldMatch": {
            "Data.DateRange": {
                    "PeriodFrom": PeriodFrom,
                    "PeriodTo": PeriodTo
            }
        }
    }

    response = requests.post(url, headers=headers, verify=False, json=data)
    response.raise_for_status()

    data = response.json()

    budgets = pd.json_normalize(data)

    budgets = budgets[[
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
    ]]

    budgets.columns = [
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
    ]

    budgets['ts'] = dt.datetime.now()

    print(budgets)

    write_data(budgets, 'stage_optimatica_budgets', 'Id')


    budget_items = []

    for budget in data:
        try:
            budget_item = pd.json_normalize(
                budget,
                record_path=["Data", "Data.YearPlanItems", "Value"],
                meta=["Id"],
                meta_prefix='budget_'
            )
            budget_items.append(budget_item)
        except KeyError:
            continue

    budget_items = pd.concat(budget_items, ignore_index=True)

    print(budget_items)

    budget_items['ts'] = dt.datetime.now()

    write_data(budget_items, 'stage_optimatica_budget_items', 'budget_Id')


def get_items(PeriodFrom, PeriodTo):
    
    items_ids = pd.read_sql(
        f"""
        SELECT bi.Id FROM sttgaz.stage_optimatica_budgets AS b
        JOIN sttgaz.stage_optimatica_budget_items AS bi
            ON b.Id = bi.budget_Id
        WHERE b.PeriodFrom = '{PeriodFrom}' AND b.PeriodTo = '{PeriodTo}'
        """,
        engine
    )

    url = base_url + 'business-objects/query'
    headers = {
        'Authorization': f'Bearer {get_token()}'
    }
    json = {
        "Ids": list(items_ids['Id'].values)
    }
    
    response = requests.post(url, headers=headers, verify=False, json=json)
    response.raise_for_status()

    items = pd.json_normalize(
        response.json(),
        record_path=["Data", "Data.YearPlan", "Value"],
        record_prefix='Budget.',
        meta=[
            "Id",
            "Number",
            "Status.CreatedAt",
            "Status.CreatedBy.Name",
            "Data.Data.Month.Text",
            "Data.Data.Media.Text",
            "Data.Data.Model.Text",
            "Data.Data.TotalPrice.Value",
        ],
        errors='ignore',
    )

    items.columns = [
        "Budget_Id",
        "Budget_Type_Name",
        "Budget_Type_Code",
        "Budget_Class_Code",
        "Id",
        "Number",
        "CreatedAt",
        "CreatedBy_Name",
        "Month",
        "Media",
        "Model",
        "TotalPrice",
    ]

    print(items)
              







start_time = time.time()

# get_budgets("2021-01-01T00:00:00+03:00", "2021-12-31T00:00:00+03:00")
get_items("2021-01-01T00:00:00+03:00", "2021-12-31T00:00:00+03:00")

print('Вренмя выполнения', time.time() - start_time)