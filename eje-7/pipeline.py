import pandas as pd
from sqlalchemy import create_engine

# ==============================
# CONFIG
# ==============================
engine= create_engine("postgresql://postgres:@localhost:5432/data_business")

LOAD_DATA=False
LOAD_TABLES=False

# ==============================
# LEER DATA
# ==============================

df = pd.read_csv("./data/business.csv")
# print(df.head())
# print(df.info())
print("--------------------------------------")

# ==============================
# RAW - GUARDAR TODO
# ==============================
    # COLUMNS -> LOWERCASE , SPLIT
df.columns = df.columns.str.strip().str.lower().str.replace(" ","_") 
#colocar id
df["id"] = range(1, len(df) + 1)
df= df[["id","product_type","net_quantity","gross_sales","discounts","returns","total_net_sales"]]
if LOAD_DATA:
    df.to_sql("business_raw", engine,if_exists="replace", index=False)
    print("Datos Cargados Correctamente")
#print(df.head())
print("--------------------------------------")


# ==============================
# VALIDACION
# ==============================
business_raw = pd.read_sql("select * from business_raw",engine)

# print(business_raw.head())
# print(business_raw.info())
# print(business_raw.isnull().sum())
# print(business_raw.duplicated().sum())
# print(business_raw.columns)
print("--------------------------------------")

# ==============================
# LIMPIEZA
# ==============================
# print(business_raw.isnull().sum())
# print(business_raw.duplicated().sum())
# print(business_raw[business_raw["product_type"].isnull()])
#eliminar los nulos
business_raw = business_raw.dropna(subset=["product_type"])
# print(business_raw.isnull().sum())
# print(business_raw.duplicated().sum())
# print(business_raw[business_raw["product_type"].isnull()])

#checar columnas negativas
# print(business_raw[business_raw["net_quantity"] < 0])
business_raw = business_raw[business_raw["net_quantity"] >= 0]
# print(business_raw[business_raw["net_quantity"] < 0])
# print(business_raw[business_raw["total_net_sales"] < 0])
# print(business_raw[business_raw["gross_sales"] < 0])
# ==============================
# SEPARACION MODELADO
# ==============================

#TABLE PRODUCT_TYPE
products_type = business_raw[["product_type"]].drop_duplicates().reset_index(drop=True)
products_type["product_id"]=products_type.index + 1
products_type=products_type[["product_id","product_type"]]
business_raw= business_raw.merge(products_type,on=["product_type"], how="left")
# print(product_type.head())
# print(business_raw.head())

#Table Sales
sales= business_raw[["net_quantity","gross_sales","discounts","returns","total_net_sales","product_id"]]
sales["sale_id"] = sales.index + 1
sales=sales[["sale_id","product_id","net_quantity","gross_sales","discounts","returns","total_net_sales"]]
# print(sales.head())

if LOAD_TABLES:
    # products_type.to_sql("products_type",engine,if_exists="append",index=False)
    sales.to_sql("sales",engine,if_exists="append",index=False)
    print("Datos insertados correctamente")