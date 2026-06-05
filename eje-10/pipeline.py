import pandas as pd
from sqlalchemy import create_engine

# ==============================
# CONFIG
# ==============================
engine = create_engine("postgresql://postgres:@localhost:5432/data_analy_bus")

LOAD_DATA=False
LOAD_TABLES=False
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

print(df.info())

if LOAD_DATA:
    df.to_sql("jobs_data",engine,if_exists="replace",index=False)
    print("Datos cargados correctamente")

# ==============================
# CHECK
# ==============================
buss_raw= pd.read_sql("SELECT * FROM jobs_data",engine)

print("--------------------------------------")
# print(buss_raw.info())

# #separte salary in two columnas called min and max
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
print("--------------------------------------")
# ==============================
# LIMPIEZA
# ==============================