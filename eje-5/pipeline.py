import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Text

# ==============================
# CONFIG
# ==============================
engine = create_engine("postgresql://postgres:@localhost:5432/ecomerce_analy_db")
chunksize = 1000
file_path = "./data/online_retail.csv"

# ==============================
# CARGA (BRONZE)
# ==============================

# for i, chunk in enumerate(pd.read_csv(file_path, dtype=str, chunksize=chunksize)):

#     # limpiar nombres de columnas
#     chunk.columns = chunk.columns.str.lower()

#     # opcional: limpiar espacios en strings
#     chunk = chunk.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

#     # crear tabla en el primer chunk
#     if i == 0:
#         chunk.to_sql(
#             "ecomerce_analy_raw",
#             engine,
#             if_exists="replace",  # crea desde cero
#             index=False,
#             dtype={
#                 "invoiceno": Text(),
#                 "stockcode": Text(),
#                 "description": Text(),
#                 "quantity": Text(),
#                 "invoicedate": Text(),
#                 "unitprice": Text(),
#                 "customerid": Text(),
#                 "country": Text()
#             }
#         )
#     else:
#         chunk.to_sql(
#             "ecomerce_analy_raw",
#             engine,
#             if_exists="append",
#             index=False
#         )

# print("✅ Datos cargados en BRONZE (raw) correctamente")

# ==============================
# 1. EXTRACCIÓN (READ)
# ==============================

ecommerce_raw = pd.read_sql(
    "SELECT * FROM ecomerce_analy_raw WHERE customerid IS NOT NULL",engine
)
#convertir tipos
ecommerce_raw["quantity"]= pd.to_numeric(ecommerce_raw["quantity"],errors="coerce")
ecommerce_raw["unitprice"]= pd.to_numeric(ecommerce_raw["unitprice"],errors="coerce")
ecommerce_raw["customerid"]= pd.to_numeric(ecommerce_raw["customerid"],errors="coerce")
ecommerce_raw["invoicedate"]= pd.to_datetime(ecommerce_raw["invoicedate"], format="%d/%m/%y %H:%M",errors="coerce")

#delete null
ecommerce_raw = ecommerce_raw.dropna(subset=["quantity","unitprice"])
#eliminar precios negativos
ecommerce_raw = ecommerce_raw[ecommerce_raw["unitprice"]>0]
#esto eliminaria las devoluciones o cancelaciones
# ecommerce_raw = ecommerce_raw[ecommerce_raw["quantity"]>0]

#nuevas columnas total_price = quantity * unitprice

# limpiar texto
text_cols=["invoiceno","stockcode","description","country"]
for col in text_cols:
    ecommerce_raw[col].astype(str).str.strip()

# devoluciones
df_return = ecommerce_raw[ecommerce_raw["quantity"]<0]
# ventas
df_sales = ecommerce_raw[ecommerce_raw["quantity"]>0]

# print("Ventas:")
# print(df_sales.head())

# # print("Devoluciones:")
# print(df_return.head())


# ==============================
# 2. TRANSFORMACIÓN (SILVER)
# ==============================

# ---------COUNTRY------------
ecommerce_raw["country"] = ecommerce_raw["country"].fillna("unknown")

#Table country
country = ecommerce_raw[["country"]].drop_duplicates().reset_index(drop=True)
country["country_id"] = country.index + 1
# country=country.rename(columns={"country":"name"})
country=country[["country_id","country"]]

# print(country.head())

#merge
ecommerce_raw = ecommerce_raw.merge(country,on=["country"], how="left")

# print(ecommerce_raw.head())

# ---------CUSTOMERS------------
#Table customers
customers = ecommerce_raw[["customerid","country_id"]].drop_duplicates().reset_index(drop=True)
customers["customer_id"] = customers.index + 1
customers=customers[["customer_id","customerid","country_id"]]
# customers = customers.rename(columns={"customerid":"user"})
ecommerce_raw = ecommerce_raw.merge(customers[["customer_id","customerid"]],on="customerid", how="left")
print(ecommerce_raw.head())
# print(customers.head())

# ---------PRODUCTS------------
#Table products
products = ecommerce_raw[["stockcode","description"]].drop_duplicates().reset_index(drop=True)
products["product_id"] = products.index + 1
products=products[["product_id","stockcode","description"]]
ecommerce_raw = ecommerce_raw.merge(products[["product_id","stockcode"]],on="stockcode", how="left")
# print(ecommerce_raw.head())

# ---------TRANSACTIONS------------

#Table transaction
transactions=ecommerce_raw[["invoiceno","customer_id","invoicedate"]].drop_duplicates().reset_index(drop=True)
transactions["transaction_id"] =transactions.index + 1

transactions["type"] = ecommerce_raw["quantity"].apply(lambda x: "return" if (x < 0) else "sale")
transactions = transactions[["transaction_id","invoiceno","customer_id","invoicedate","type"]]
# print(transactions.head())

#Table transaction_details
transactions_details=ecommerce_raw[["invoiceno","product_id","quantity","unitprice"]]
transactions_details = transactions_details.merge(transactions[["transaction_id","invoiceno"]],on="invoiceno",how="left")
transactions_details["transaction__details_id"] =transactions_details.index + 1

transactions_details = transactions_details[
    ["transaction__details_id","transaction_id","product_id","quantity","unitprice"]
]
# print(transactions_details.head())


# Control para evitar insertar datos accidentalmente
LOAD_DATA = False

# Insertar datos en tablas (SILVER)
# if_exists="append" agrega sin borrar datos previos
if LOAD_DATA:
    country.to_sql("country",engine, if_exists="append", index=False)
    customers.to_sql("customers", engine, if_exists="append", index=False)
    products.to_sql("products", engine, if_exists="append", index=False)
    transactions.to_sql("transactions", engine, if_exists="append", index=False)
    transactions_details.to_sql("transactions_details", engine, if_exists="append", index=False)

    print("Datos cargados a PostgreSQL")
