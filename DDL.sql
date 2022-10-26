DROP TABLE IF EXISTS sttgaz.stage_optimatica_budgets;

CREATE TABLE sttgaz.stage_optimatica_budgets (
    "Id" VARCHAR(100),
    "Number" VARCHAR(30),
    "ObjectType_Code" VARCHAR(100),
    "ObjectType_Name" VARCHAR(300),
    "ObjectClass_Code" VARCHAR(100),
    "ObjectClass_Name" VARCHAR(300),
    "CreatedAt" TIMESTAMP WITH TIME ZONE,
    "CreatedBy" VARCHAR(300),
    "Deleted" VARCHAR(100),
    "Frozen" VARCHAR(100),
    "Dealer" VARCHAR(6000),
    "DealerCity" VARCHAR(6000),
    "DealerLegalAddress" VARCHAR(6000),
    "Year" VARCHAR(10),
    "PeriodFrom" TIMESTAMP WITH TIME ZONE,
    "PeriodTo" TIMESTAMP WITH TIME ZONE,
    "Specialization" VARCHAR(300),
    "ZoneAM" VARCHAR(300),
    "RegionalSalesManager" VARCHAR(300),
    "MinimumBudget" REAL,
    "PlanBudget" REAL,
    "FactBudget" REAL,
    "Budget" REAL,
    "YearPlanItems" VARCHAR(6000),
    "State" VARCHAR(100),
    "Deadline" TIMESTAMP,
    "Activity" TIMESTAMP,
    "ts" TIMESTAMP   
);

DROP TABLE IF EXISTS sttgaz.stage_optimatica_budget_items;

CREATE TABLE sttgaz.stage_optimatica_budget_items(
    "Id" VARCHAR(100),
    "Name" VARCHAR(6000),
    "ObjectType" VARCHAR(100),
    "ObjectClass" VARCHAR(100),
    "budget_Id" VARCHAR(100),
    "ts" TIMESTAMP   
);


CREATE TABLE sttgaz.stage_optimatica_items(
    "Budget_Id" VARCHAR(100),
    "BudgetType_Name" VARCHAR(300),
    "BudgetType_Code" VARCHAR(100),
    "Budget_Class_Code" VARCHAR(100),
    "Id" VARCHAR(100),
    "Number" VARCHAR(30),
    "CreatedAt" TIMESTAMP WITH TIME ZONE,
    "CreatedBy_Name" VARCHAR(300),
    "Month" VARCHAR(100),
    "Media" VARCHAR(300),
    "Model" VARCHAR(300),
    "TotalPrice" REAL,
    "ts" TIMESTAMP  
);

