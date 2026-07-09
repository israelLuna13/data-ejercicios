import pandas as pd
from sqlalchemy import create_engine

# ==============================
# CONFIG
# ==============================
engine = create_engine("postgresql://postgres:@localhost:5432/data_amazon")

LOAD_DATA=False
LOAD_TABLES=False
# ==============================
# READ
# ==============================
# df= pd.read_csv("./data/Amazon Sale Report.csv")
# print(df.head())
# print(df.info())

# ==============================
#  RAW - GUARDAR TODO
# ==============================
# COLUMNS -> LOWERCASE
#Eliminar columnas basura

# df=df.drop(columns=['Unnamed: 22'])
# df.columns= df.columns.str.strip().str.lower().str.replace(" ","_").str.replace("-","_")

if LOAD_DATA:
    df.to_sql("data_amazon",engine, if_exists="replace" ,index=False)
    print("Data cargados correctamente")
# print(df.columns)

# ==============================
# CHECK
# ==============================

data_raw = pd.read_sql("SELECT * FROM data_amazon", engine)
# print(data_raw.info())
#convertir date str a date
# print(data_raw['date'].isna().sum())
data_raw["date"] = pd.to_datetime(data_raw["date"])
# print(data_raw["date"].dtype)

# Columns int/float
# print(data_raw['index'].isna().sum())

#checar valores nulos y 0 de amount
# print(data_raw['amount'].isna().sum())
# print(data_raw["amount"].head(20))
# print(data_raw["amount"].describe())

# print(data_raw[data_raw["amount"].isna()]["status"].value_counts()
# )
# print(data_raw[data_raw["amount"] == 0]["status"].value_counts())
# print(data_raw[data_raw["amount"] == 0].head(20))
# print(data_raw[data_raw["amount"] == 0][
#     ["status", "qty", "category", "sku"]
# ].head(20))
# print(data_raw[data_raw["amount"] == 0]["promotion_ids"].value_counts().head(5))
# print((data_raw["amount"] == 0).sum())

#valores negativos
numeric_cols= data_raw.select_dtypes(include=["int64","float64"]).columns
for col in numeric_cols:
    negative_values=(data_raw[col] < 0).sum()
    print(f"{col}:{negative_values} negativos")
print("--------------------------------------")

#Columnas str
columns_na= data_raw.select_dtypes(include=["str","object"]).columns
for col in columns_na:
    na_columns=(data_raw[col]).isna().sum()
    print(f"{col}:{na_columns} null")
#colocar unknown en los null de city, state y country y code postal
data_raw["ship_city"] = data_raw["ship_city"].fillna('unknown')
data_raw["ship_state"] = data_raw["ship_state"].fillna('unknown')
data_raw["ship_country"] = data_raw["ship_country"].fillna('unknown')
data_raw["ship_postal_code"] = data_raw["ship_postal_code"].fillna('unknown')

# print(data_raw[
#     data_raw["ship_city"].isna()
# ][["order_id", "status", "amount"]])
# print("--------------------------------------")

# print("--------------------------------------")

# for col in data_raw.select_dtypes(include="object").columns:
#     print(f"\n{col}")
#     print(data_raw[col].nunique())
# print("--------------------------------------")

# print(data_raw.select_dtypes(include="object").apply(
#     lambda x: x.astype(str).str.strip().value_counts().head()
# ))
# print("--------------------------------------")

# print(data_raw["status"].value_counts(dropna=False)
# )
# print("--------------------------------------")

# print(data_raw["category"].value_counts(dropna=False)
# )
print("--------------------------------------")
# ==============================
# SEPARACION MODELADO
# ==============================

#location
dimCity = data_raw[["ship_city"]].drop_duplicates().reset_index(drop=True)
dimCity["id_city"] = dimCity.index + 1
dimCity = dimCity[["id_city","ship_city"]]
data_raw = data_raw.merge(dimCity, on=["ship_city"], how="left")

dimState = data_raw[["ship_state"]].drop_duplicates().reset_index(drop=True)
dimState["id_state"] = dimState.index + 1
dimState = dimState[["id_state","ship_state"]]
data_raw = data_raw.merge(dimState, on=["ship_state"], how="left")


dimCountry = data_raw[["ship_country"]].drop_duplicates().reset_index(drop=True)
dimCountry["id_country"] = dimCountry.index + 1
dimCountry = dimCountry[["id_country","ship_country"]]
data_raw = data_raw.merge(dimCountry, on=["ship_country"], how="left")


#product

dimSize = data_raw[["size"]].drop_duplicates().reset_index(drop=True)
dimSize["id_size"] = dimSize.index + 1
dimSize = dimSize[["id_size","size"]]
data_raw = data_raw.merge(dimSize, on=["size"], how="left")


dimCategory = data_raw[["category"]].drop_duplicates().reset_index(drop=True)
dimCategory["id_category"] = dimCategory.index + 1
dimCategory = dimCategory[["id_category","category"]]
data_raw = data_raw.merge(dimCategory, on=["category"], how="left")


dimProduct = data_raw[["style","sku","asin"]].drop_duplicates().reset_index(drop=True)
dimProduct["id_product"] = dimProduct.index + 1
dimProduct = dimProduct[["id_product","style","sku","asin"]]
data_raw = data_raw.merge(dimProduct, on=["style","sku","asin"], how="left")


#fact
dimFact=data_raw[["order_id","date","qty","amount","id_product","id_category","id_size","id_city","id_state","id_country","ship_postal_code"]]

dimFact["fact_id"] = dimFact.index + 1
dimFact=dimFact[["fact_id","order_id","date","qty","amount","id_product","id_category","id_size","id_city","id_state","id_country","ship_postal_code"]]

print(dimFact)