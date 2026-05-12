import pandas as pd
from sqlalchemy import create_engine

# ==============================
# CONFIG
# ==============================
engine= create_engine("postgresql://postgres:@localhost:5432/data_delivery")

LOAD_DATA=False
LOAD_TABLES=False

# ==============================
# LEER DATA
# ==============================

df = pd.read_csv("./data/cleaned_test.csv")
# print(df.head())
# print(df.info())
print("--------------------------------------")

# ==============================
# RAW - GUARDAR TODO
# ==============================
# COLUMNS -> LOWERCASE
df.columns= df.columns.str.strip().str.lower()
# print(df.head())
if LOAD_DATA:
    df.to_sql("raw_delivery",engine, if_exists="replace", index=False)
    print("Datos cargados correctamente")

# ==============================
# VALIDACION
# ==============================
print("--------------------------------------")

delivery_raw = pd.read_sql("select * from raw_delivery", engine)
#print(delivery_raw.shape)
# print(delivery_raw.head())
# print(delivery_raw.info())
# print(delivery_raw.isnull().sum())
# print(delivery_raw[(delivery_raw["restaurant_latitude"] == 0)])
# print(delivery_raw[(delivery_raw["restaurant_longitude"] == 0)])
# print(delivery_raw.columns)
# print(delivery_raw[delivery_raw["delivery_person_id"].duplicated()])
# print(delivery_raw["delivery_person_id"].duplicated().sum())
# print(delivery_raw["delivery_person_id"].value_counts())
# print(delivery_raw["weather"].unique())
# print(delivery_raw["city"].value_counts())
# print(delivery_raw[(delivery_raw["delivery_person_ratings"] > 5)])
# print(delivery_raw[(delivery_raw["delivery_person_ratings"] <0)])


#checar columnas negativas
# numeric_cols = delivery_raw.select_dtypes(include="number").columns

# for col in numeric_cols:
#     negatives = (delivery_raw[col] < 0).sum()

#     if negatives > 0:
#         print(f"{col}: {negatives} negativos")
# ==============================
# LIMPIEZA
# ==============================
#Eliminar null
delivery_raw = delivery_raw.dropna(subset=["city"])
#eliminar coordenadas con 0
delivery_raw = delivery_raw[
    (delivery_raw["restaurant_latitude"] != 0) &
    (delivery_raw["restaurant_longitude"] != 0) &
    (delivery_raw["delivery_location_latitude"] != 0) &
    (delivery_raw["delivery_location_longitude"] != 0)]
#eliminar ratings con mas de 5
delivery_raw=delivery_raw[(delivery_raw["delivery_person_ratings"]) < 6]
# print(delivery_raw[(delivery_raw["delivery_person_ratings"] > 5)])

# print(delivery_raw.isnull().sum())
# print(delivery_raw[(delivery_raw["restaurant_latitude"] == 0)])
# print(delivery_raw[(delivery_raw["restaurant_longitude"] == 0)])
# print(delivery_raw[(delivery_raw["delivery_location_latitude"] == 0)])
# print(delivery_raw[(delivery_raw["delivery_location_longitude"] == 0)])
print(delivery_raw.shape)

print("--------------------------------------")
# delivery_raw= delivery_raw.dropna(subset=[""])