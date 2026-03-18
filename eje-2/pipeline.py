import pandas as pd
from sqlalchemy import create_engine

# ==============================
# 1. EXTRACCIÓN (READ)
# ==============================

# Leer el dataset desde CSV
df = pd.read_csv("data/orders.csv")

# Normalizar nombres de columnas (buena práctica)
df.columns = df.columns.str.lower()

# Ver primeras filas para validar
print(df.head())


# ==============================
# 2. TRANSFORMACIÓN
# ==============================

# ---------- CUSTOMERS ----------
# Crear tabla de clientes basada en combinaciones únicas
# (en este dataset "sex + smoker" representa un "tipo de cliente")
customers = df[["sex", "smoker"]].drop_duplicates().reset_index(drop=True)

# Crear ID único para cada cliente
customers["customer_id"] = customers.index + 1

# Reordenar columnas (id primero)
customers = customers[["customer_id", "sex", "smoker"]]

# Hacer merge para asignar customer_id a cada fila original
df = df.merge(customers, on=["sex", "smoker"], how="left")


# ---------- ORDERS ----------
# Crear ID único para cada orden
df["order_id"] = df.index + 1

# Seleccionar columnas relevantes para la tabla de órdenes
orders = df[[
    "order_id",
    "customer_id",
    "total_bill",
    "tip",
    "day",
    "time",
    "size"
]]

# Crear nueva columna derivada (feature engineering)
orders["total_with_tip"] = orders["total_bill"] + orders["tip"]


# ---------- PRODUCTS ----------
# Crear tabla de productos (en este caso "day" se trata como producto)
products = df[["day"]].drop_duplicates().reset_index(drop=True)

# Crear ID único para cada producto
products["product_id"] = products.index + 1

# Reordenar columnas
products = products[["product_id", "day"]]

# Relacionar orders con products mediante "day"
orders = orders.merge(products, on="day", how="left")

# Eliminar columna redundante (ya tenemos product_id)
orders = orders.drop(columns=["day"])


# ==============================
# 3. VALIDACIÓN (DEBUG)
# ==============================

print('------------------')
print(df.head())
print('------------------')

# Validar tamaños de tablas
print("Customers shape:", customers.shape)
print('------------------')

print("Products shape:", products.shape)
print('------------------')

print("Orders shape:", orders.shape)


# ==============================
# 4. LOAD (CARGA A BASE DE DATOS)
# ==============================

# Crear conexión a PostgreSQL
engine = create_engine("postgresql://postgres:@localhost:5432/orders_db")

# Insertar datos en tablas
# append = agrega datos sin borrar lo anterior
products.to_sql("products", engine, if_exists="append", index=False)
customers.to_sql("customers", engine, if_exists="append", index=False)
orders.to_sql("orders", engine, if_exists="append", index=False)

print("Datos cargados a PostgreSQL")