import pandas as pd
from sqlalchemy import create_engine

# ==============================
# CONFIG
# ==============================
engine= create_engine("postgresql://postgres:@localhost:5432/data_udemy")

LOAD_DATA=False
LOAD_TABLES=True
# ==============================
# READ DATA
# ==============================

df = pd.read_csv("./data/udemy_output_All_Business_p1_p626.csv")
# print(df.head())
# print(df.info())
print("--------------------------------------")

#  RAW - GUARDAR TODO
# ==============================
# COLUMNS -> LOWERCASE
df.columns= df.columns.str.strip().str.lower().str.replace("__","_")
# print(df.head())
# print(df.info())

if LOAD_DATA:
    df.to_sql("courses",engine, if_exists="replace", index=False)
    print("Datos cargados correctamente")

# ==============================
# CHECK
# ==============================
courses_raw = pd.read_sql("select * from courses",engine)

# print(courses_raw.head())
# print("---------------------")
# print(courses_raw.info())
# print("---------------------")
# print(courses_raw.isnull().sum())
# print("---------------------")
# print(courses_raw.duplicated().sum())
# print("---------------------")
# print(courses_raw.columns)
# print("---------------------")
# #revisar columnas numericas
# print(courses_raw[courses_raw["avg_rating"] > 5])
# print(courses_raw[courses_raw["avg_rating_recent"] > 5])


# print("---------------------")

# print(
#     courses_raw[
#         [
#             "discount_price_amount",
#             "discount_price_currency",
#             "price_detail_amount",
#             "price_detail_currency"
#         ]
#     ].head()
# )
# print("---------------------")
# numeric_cols = courses_raw.select_dtypes(include=["int64","float64"]).columns

# for col in numeric_cols:
#     negative_values=(courses_raw[col] < 0).sum()
#     print(f"{col}:{negative_values} negativos")
# print("---------------------")

# #revisar columnas strings
# print(
#     courses_raw[
#         [
#             "title",
#             "url",
#             "created",
#             "published_time"
#         ]
#     ].head(10)
# )
# print(courses_raw["id"].duplicated().sum())
# ==============================
# LIMPIEZA
# ==============================
print("---------------------")

#solamente cambiar fechas de str a date
courses_raw["created"]= pd.to_datetime(
    courses_raw["created"],
    errors="coerce"
)
courses_raw["published_time"]= pd.to_datetime(
    courses_raw["published_time"],
    errors="coerce"
)

# print(courses_raw["created"].isna().sum())
# print(courses_raw["published_time"].isna().sum())
# print(courses_raw.info())

# ==============================
# SEPARACION MODELADO
# ==============================
fact_udemy = courses_raw[['id', 'title', 'url', 'is_paid', 'num_subscribers', 'avg_rating',
       'avg_rating_recent', 'rating', 'num_reviews', 'is_wishlisted',
       'num_published_lectures', 'num_published_practice_tests', 'created',
       'published_time', 'discount_price_amount',
       'price_detail_amount']]

if LOAD_TABLES:
    fact_udemy.to_sql("fact_udemy",engine,if_exists="append", index=False)
    print("Datos cargados correctamente")