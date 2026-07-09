import pandas as pd
from sqlalchemy import create_engine

LOAD_DATA=False
LOAD_TABLES=False
# ==============================
# CONFIG
# ==============================
engine= create_engine("postgresql://postgres:@localhost:5432/data_ecomerce")

# ==============================
# READ
# ==============================
# df= pd.read_csv("./data/ecommerce_customer_behavior_dataset.csv"
#                 )
# print(df.info())
# print("-----------------")
# print(df.head())
# print("-----------------")
# print(df.describe())
# print("-----------------")
# print(df.sample(10))
# print("-----------------")

#  RAW - GUARDAR TODO
# ==============================
# COLUMNS -> LOWERCASE
# df.columns= df.columns.str.strip().str.lower().str.replace(" ","_")
# if LOAD_DATA:
#     df.to_sql("orders_data",engine, if_exists="replace", index=False)
#     print("Data cargados correctamente")
# print(df.info())

# ==============================
# CHECK/LIMPIEZA
# ==============================
data_raw = pd.read_sql("SELECT * FROM orders_data", engine)

print(data_raw.info())
print("-----------------")
print(data_raw.head())
print("-----------------")
print(data_raw.describe())
print("-----------------")
print(data_raw.sample(10))
print("-----------------")

# Columnas int
columns_int = data_raw.select_dtypes(include=["int64","float64"]).columns

for col in columns_int:
    negative_value=(data_raw[col]< 0).sum()
    print(f"{col}:{negative_value} negativos")
    na_values=data_raw[col].isna().sum()
    print(f"{col}:{na_values} nulos")

print("-----------------")

#Columna date
data_raw["date"]=pd.to_datetime(data_raw["date"])
# print(data_raw["date"].head(20))
# print("-----------------")
# print(data_raw["date"].min())
# print(data_raw["date"].max())

# data_raw["year"] = data_raw["date"].dt.year
# data_raw["month"] = data_raw["date"].dt.month
# print(data_raw["year"].value_counts().sort_index())

#Columnas str
columns_str = data_raw.select_dtypes(include=["str"]).columns

for col in columns_str:
    print(f"\n{col}")
    print(f"Nulos: {data_raw[col].isna().sum()}")
    print(f"Unicos:{ len(data_raw[col].unique())}")

    # Espacios al inicio o final
    spaces = (
        data_raw[col]
        .dropna()
        .astype(str)
        .apply(lambda x: x != x.strip())
        .sum()
    )
    print(f"Con espacios: {spaces}")

    # Cadenas vacía
    empty = (
        data_raw[col]
        .dropna()
        .astype(str)
        .str.strip()
        .eq("")
        .sum()
    )
    print(f"Cadenas vacías: {empty}")

    if data_raw[col].nunique(dropna=False) <= 20:
        print(data_raw[col].value_counts(dropna=False))

# ==============================
# ==============================
dim_payment = data_raw[["payment_method"]].drop_duplicates().reset_index(drop=True)
dim_payment["id_payment"] = dim_payment.index + 1
dim_payment = dim_payment[["id_payment","payment_method"]]
data_raw = data_raw.merge(dim_payment, on=["payment_method"], how="left")

dim_device = data_raw[["device_type"]].drop_duplicates().reset_index(drop=True)
dim_device["id_device"] = dim_device.index + 1
dim_device = dim_device[["id_device","device_type"]]
data_raw = data_raw.merge(dim_device, on=["device_type"], how="left")

dim_customer = data_raw[["customer_id","age","gender","city","is_returning_customer"]].drop_duplicates().reset_index(drop=True)
dim_customer["id_cust"] = dim_customer.index + 1
dim_customer = dim_customer[["id_cust","customer_id","age","gender","city","is_returning_customer"]]
data_raw = data_raw.merge(dim_customer, on=["customer_id","age","gender","city","is_returning_customer"], how="left")
# dim_city = data_raw[["city"]].drop_duplicates().reset_index(drop=True)
# dim_city["id_city"] = dim_city.index + 1
# dim_city = dim_city[["id_city","city"]]
# data_raw = data_raw.merge(dim_city, on=["city"], how="left")
dim_category = data_raw[["product_category"]].drop_duplicates().reset_index(drop=True)
dim_category["id_product"] = dim_category.index + 1
dim_category = dim_category[["id_product","product_category"]]
data_raw = data_raw.merge(dim_category, on=["product_category"], how="left")

dim_fact= data_raw[["order_id","date","id_cust","id_device","id_payment","unit_price","quantity","discount_amount","total_amount","delivery_time_days","customer_rating"]]
dim_fact["id_fact"]= dim_fact.index+1
dim_fact= dim_fact[["id_fact","date","order_id","id_cust","id_device","id_payment","unit_price","quantity","discount_amount","total_amount","delivery_time_days","customer_rating"]]
print(dim_fact)

# dim_gender = data_raw[["gender"]].drop_duplicates().reset_index(drop=True)
# dim_gender["id_gender"] = dim_gender.index + 1
# dim_gender = dim_gender[["id_gender","gender"]]
# data_raw = data_raw.merge(dim_gender, on=["gender"], how="left")

# print(data_raw.groupby("customer_id").size().sort_values(ascending=False).head())

# print(data_raw.groupby("customer_id")[["age", "gender", "city"]].nunique())