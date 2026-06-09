import pandas as pd
from sqlalchemy import create_engine

# ==============================
# CONFIG
# ==============================
engine = create_engine("postgresql://postgres:@localhost:5432/data_analy_bus")

LOAD_DATA=False
LOAD_TABLES=True
# ==============================
# SE ARREGLO EL PROBLEMA DONDE SE RECORRIAN LOS DATOS HACIA LA DERECHA ELIMINANDO LAS DOS PRIMERAS COLUMNAS
# ==============================
# df= pd.read_csv("./data/BusinessAnalyst.csv")

# df_fixed = df.copy()

# for i in range(3692, len(df_fixed)):
#     valores = df_fixed.loc[i].tolist()
#     nuevo = [str(i), str(i)] + valores[:-2]
#     df_fixed.loc[i] = nuevo


# df_fixed.to_csv("jobs_fixed.csv", index=False)
# print("--------------------------------------")
# ==============================
# READ
# ==============================

df= pd.read_csv("./data/jobs_fixed.csv")
# print(df.head())
# print(df.info())
# ==============================
#  RAW - GUARDAR TODO
# ==============================
# COLUMNS -> LOWERCASE
df.columns= df.columns.str.strip().str.lower().str.replace(" ","_")

df.rename(columns={'unnamed:_0':"id","index":"job_id"}, inplace=True)

# print(df.info())

if LOAD_DATA:
    df.to_sql("jobs_data",engine,if_exists="replace",index=False)
    print("Datos cargados correctamente")

# ==============================
# CHECK
# ==============================
buss_raw= pd.read_sql("SELECT * FROM jobs_data",engine)

print("--------------------------------------")
# print(buss_raw.info())

#eliminar texto y separar el salario en min y max
buss_raw["salary_estimate"]= (
    buss_raw["salary_estimate"]
    .str.replace(r"\s*\(Glassdoor est\.\)","", regex=True)
)
# print(buss_raw["salary_estimate"].sample(20))
buss_raw["salary_min"]= (
    buss_raw["salary_estimate"]
    .str.extract(r"\$(\d+)K")[0]
    .astype(int)
    * 1000
)
buss_raw["salary_max"]= (
    buss_raw["salary_estimate"]
    .str.extract(r"\$\d+K-\$(\d+)K")[0]
    .astype(int)
    * 1000
)

#check negative columns values

# ratings_invalidos= buss_raw[
#     (buss_raw["rating"]<0) |
#     (buss_raw["rating"]>5)
# ]
# print(ratings_invalidos["rating"])
# print(buss_raw[(buss_raw["founded"] < 0)]["founded"])
# print(buss_raw[(buss_raw["founded"] >2025)])
# print(buss_raw[(buss_raw["salary_max"] <= 0)])
# print(buss_raw[(buss_raw["salary_min"] <=0)])

#reemplazar los valores -1 de rating y founded con nan porque quiere decir que son datos nos disponibles
buss_raw["rating"] = buss_raw["rating"].replace(-1, pd.NA)
buss_raw["founded"] = buss_raw["founded"].replace(-1, pd.NA)
buss_raw[["rating", "founded"]].isna().sum()

# for col in buss_raw.columns:
#     print(col, (buss_raw[col].astype(str)== "-1").sum())
buss_raw = buss_raw.replace("-1", pd.NA)

#check str columns
#separar ciudad y estado
# buss_raw[["city", "state"]] = buss_raw["location"].str.split(", ", expand=True)
# 
split_location = buss_raw["location"].str.split(", ",n=1, expand=True)

buss_raw["city"]= split_location[0]
buss_raw["state"]=split_location[1]

buss_raw["revenue"] = buss_raw["revenue"].replace(
    "Unknown / Non-Applicable",
    pd.NA
)

# print(
#     buss_raw["revenue"]
#     .value_counts(dropna=False)
# )
print(buss_raw.info())
print("--------------------------------------")
# ==============================
# ==============================

#table salary
salary= buss_raw[["salary_min","salary_max"]].drop_duplicates().reset_index(drop=True)
salary["salary_id"]= salary.index + 1
salary=salary[["salary_id","salary_min","salary_max"]]
buss_raw= buss_raw.merge(salary,on=["salary_min","salary_max"], how="left")

# print(salary.head())
# print(buss_raw.head())

#table location
location= buss_raw[["city","state"]].drop_duplicates().reset_index(drop=True)
location["location_id"]= location.index + 1
location=location[["location_id","city","state"]]
buss_raw= buss_raw.merge(location, on=["city","state"], how="left")
# print(buss_raw.head())

#table company
company= buss_raw[["headquarters","type_of_ownership","industry","sector"]].drop_duplicates().reset_index(drop=True)
company["company_id"]=company.index + 1
company=company[["company_id","headquarters","type_of_ownership","industry","sector"]]
buss_raw=buss_raw.merge(company, on=["headquarters","type_of_ownership","industry","sector"], how="left")
# print(buss_raw.head())

#fact table
fact_jobs= buss_raw[["job_title","rating","founded","location_id","company_id","salary_id","revenue"]]
fact_jobs["fact_id"]= fact_jobs.index + 1
fact_jobs= fact_jobs[["fact_id","job_title","rating","founded","revenue","location_id","company_id","salary_id"]]

# print(fact_jobs.head())


if LOAD_TABLES:
    # salary.to_sql("salary",engine,if_exists="append", index=False)
    # company.to_sql("company",engine,if_exists="append", index=False)
    # location.to_sql("location",engine,if_exists="append", index=False)
    fact_jobs.to_sql("fact_jobs",engine,if_exists="append", index=False)
    print("Datos cargados correctamente")
