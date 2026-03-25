import pandas as pd
from sqlalchemy import create_engine

# ==============================
# 1. EXTRACCIÓN (READ)
# ==============================

# Leer archivo CSV (opcional, solo para carga inicial a DB)
# df = pd.read_csv("data/ecommerce.csv")

# Convertir nombres de columnas a minúsculas para consistencia
# df.columns = df.columns.str.lower()

# Crear conexión a PostgreSQL
engine = create_engine("postgresql://postgres:@localhost:5432/ecomerce_db")

# Cargar datos crudos a tabla bronze (solo una vez)
# if_exists="append" agrega sin borrar datos existentes
# df.to_sql("ecomerce_raw", engine, if_exists="append", index=False)

# print("Datos cargados a PostgreSQL")

# Leer datos desde la tabla bronze (filtrando valores importantes no nulos)
ecomerce_raw = pd.read_sql(
    "SELECT * FROM ecomerce_raw WHERE fare is not null AND survived is not null",
    engine
)

# print(ecomerce_raw.head())


# ==============================
# 2. TRANSFORMACIÓN (SILVER)
# ==============================

# ---------CUSTOMERS------------

# Limpieza de datos: rellenar valores nulos
# Edad: usar promedio
ecomerce_raw["age"] = ecomerce_raw["age"].fillna(ecomerce_raw["age"].mean())

# Sexo: valor por defecto "unknown"
ecomerce_raw["sex"] = ecomerce_raw["sex"].fillna("unknown")

# Crear tabla de dimensión customers (valores únicos)
customers = ecomerce_raw[["sex", "age"]].drop_duplicates().reset_index(drop=True)

# Crear identificador único para cada cliente
customers["customer_id"] = customers.index + 1

# Reordenar columnas
customers = customers[["customer_id", "sex", "age"]]

# Asignar customer_id a cada registro del dataset original
ecomerce_raw = ecomerce_raw.merge(customers, on=["sex", "age"], how="left")


# ---------PRODUCTS------------

# Limpieza de datos
# pclass representa categoría (aunque aquí usas mean, normalmente sería categórico)
ecomerce_raw["pclass"] = ecomerce_raw["pclass"].fillna(ecomerce_raw["pclass"].mean())

# Crear dimensión products
products = ecomerce_raw[["pclass"]].drop_duplicates().reset_index(drop=True)

# Crear ID de producto
products["product_id"] = products.index + 1

# Reordenar columnas
products = products[["product_id", "pclass"]]

# Asignar product_id al dataset original
ecomerce_raw = ecomerce_raw.merge(products, on=["pclass"], how="left")


# ---------CHANNEL------------

# Limpieza de datos
# Canal de compra (embarked)
ecomerce_raw["embarked"] = ecomerce_raw["embarked"].fillna("unknown")

# Crear dimensión channel
channel = ecomerce_raw[["embarked"]].drop_duplicates().reset_index(drop=True)

# Crear ID de canal
channel["channel_id"] = channel.index + 1

# Reordenar columnas
channel = channel[["channel_id", "embarked"]]

# Asignar channel_id al dataset original
ecomerce_raw = ecomerce_raw.merge(channel, on=["embarked"], how="left")

# print(channel.head())


# ---------ORDERS (FACT TABLE)------------

# Crear identificador único de orden
ecomerce_raw["order_id"] = ecomerce_raw.index + 1

# Seleccionar columnas necesarias para la tabla de hechos
orders = ecomerce_raw[
    ["order_id", "customer_id", "product_id", "channel_id", "fare", "survived"]
]

# Renombrar columnas a nombres de negocio
orders = orders.rename(columns={
    "fare": "price",
    "survived": "purchased"
})

# Crear columna derivada: indicador de orden cara
orders["is_expensive"] = ecomerce_raw["fare"].apply(
    lambda x: 1 if x > 50 else 0
)

# Visualización de datos transformados
print(ecomerce_raw.head())
print(orders.head())


# ==============================
# 3. LOAD (CARGA A DB)
# ==============================

# Control para evitar insertar datos accidentalmente
LOAD_DATA = False

# Insertar datos en tablas (SILVER)
# if_exists="append" agrega sin borrar datos previos
if LOAD_DATA:
    customers.to_sql("customers", engine, if_exists="append", index=False)
    products.to_sql("products", engine, if_exists="append", index=False)
    channel.to_sql("channel", engine, if_exists="append", index=False)
    orders.to_sql("orders", engine, if_exists="append", index=False)

    print("Datos cargados a PostgreSQL")

#------------------------------
# import pandas as pd
# from sqlalchemy import create_engine

# # ==============================
# # 1. EXTRACCIÓN (READ)
# # ==============================
# # df=pd.read_csv("data/ecommerce.csv")
# # df.columns= df.columns.str.lower()

# # Crear conexión a PostgreSQL
# engine = create_engine("postgresql://postgres:@localhost:5432/ecomerce_db")

# # # Insertar datos en tablas
# # # append = agrega datos sin borrar lo anterior
# # df.to_sql("ecomerce_raw", engine, if_exists="append", index=False)

# # print("Datos cargados a PostgreSQL")

# ecomerce_raw =pd.read_sql("SELECT * FROM ecomerce_raw WHERE fare is not null AND survived is not null", engine)
# # print(ecomerce_raw.head())

# # # ==============================
# # # 2. TRANSFORMACION
# # # ==============================

# # # ---------CUSTOMER------------
# #clear data
# ecomerce_raw["age"] = ecomerce_raw["age"].fillna(ecomerce_raw["age"].mean())
# ecomerce_raw["sex"] = ecomerce_raw["sex"].fillna("unknown")
# #create table
# customers = ecomerce_raw[["sex","age"]].drop_duplicates().reset_index(drop=True)
# customers["customer_id"] = customers.index + 1
# customers = customers[["customer_id","sex","age"]]
# ecomerce_raw=ecomerce_raw.merge(customers,on=["sex","age"],how="left")
# # # ---------PRODUCTS------------
# #clear data
# ecomerce_raw["pclass"] = ecomerce_raw["pclass"].fillna(ecomerce_raw["pclass"].mean())
# products = ecomerce_raw[["pclass"]].drop_duplicates().reset_index(drop=True)
# products["product_id"] = products.index + 1
# products = products[["product_id","pclass"]]
# ecomerce_raw=ecomerce_raw.merge(products,on=["pclass"],how="left")

# # # ---------CHANNEL------------
# #clear data
# ecomerce_raw["embarked"] = ecomerce_raw["embarked"].fillna("unknown")
# channel = ecomerce_raw[["embarked"]].drop_duplicates().reset_index(drop=True)
# channel["channel_id"] = channel.index + 1
# channel = channel[["channel_id","embarked"]]
# ecomerce_raw=ecomerce_raw.merge(channel,on=["embarked"],how="left")
# # print(channel.head())
# # # ---------ORDER------------
# ecomerce_raw["order_id"] = ecomerce_raw.index + 1
# orders = ecomerce_raw[["order_id","customer_id","product_id","channel_id","fare","survived"]]
# # print(orders.head())

# orders=orders.rename(columns={"fare":"price","survived":"purchased"})
# # orders["price"] = ecomerce_raw["fare"]
# # orders["purchased"] = ecomerce_raw["survived"]
# orders["is_expensive"] = ecomerce_raw["fare"].apply(lambda x: 1 if x > 50 else 0 )

# print(ecomerce_raw.head())
# print(orders.head())

# LOAD_DATA = False

# # Insertar datos en tablas
# # append = agrega datos sin borrar lo anterior
# if LOAD_DATA:
#     customers.to_sql("customers", engine, if_exists="append", index=False)
#     products.to_sql("products", engine, if_exists="append", index=False)
#     channel.to_sql("channel", engine, if_exists="append", index=False)
#     orders.to_sql("orders", engine, if_exists="append", index=False)
#     print("Datos cargados a PostgreSQL")