import pandas as pd
from sqlalchemy import create_engine

# ==============================
# 1. EXTRACCIÓN (READ)
# ==============================
df=pd.read_csv("data/ecommerce.csv")
df.columns= df.columns.str.lower()
# ==============================
# 2. TRANSFORMACION
# ==============================

# ---------CUSTOMER------------
customers = df[["sex","age"]].drop_duplicates().reset_index(drop=True)
customers["customer_id"] = customers.index + 1
customers = customers[["customer_id","sex","age"]]

df=df.merge(customers,on=["sex","age"],how="left")
print(df.head())
# ---------PRODUCTS------------
products = df[["pclass"]].drop_duplicates().reset_index(drop=True)
products["product_id"] = products.index + 1
products = products[["product_id","pclass"]]

df=df.merge(products,on=["pclass"],how="left")
print(df.head())
print(products.head())

# ---------CHANNEL------------
channel = df[["embarked"]].drop_duplicates().reset_index(drop=True)
channel["channel_id"] = channel.index + 1
channel = channel[["channel_id","embarked"]]
df=df.merge(channel,on=["embarked"],how="left")
print(df.head())
print(channel.head())

# ---------ORDER------------
df["order_id"] = df.index + 1
print(df.head())
orders = df[["order_id","customer_id","product_id","channel_id"]]
orders["price"] = df["fare"]
orders["purchased"] = df["survived"]
orders["is_expensive"] = df["fare"].apply(lambda x: 1 if x > 50 else 0 )
print(orders.head())

# Crear conexión a PostgreSQL
engine = create_engine("postgresql://postgres:@localhost:5432/ecomerce_db")

# Insertar datos en tablas
# append = agrega datos sin borrar lo anterior
customers.to_sql("customers", engine, if_exists="append", index=False)
products.to_sql("products", engine, if_exists="append", index=False)
channel.to_sql("channel", engine, if_exists="append", index=False)
orders.to_sql("orders", engine, if_exists="append", index=False)

print("Datos cargados a PostgreSQL")