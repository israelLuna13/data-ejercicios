import pandas as pd
from sqlalchemy import create_engine

# ==============================
# 1. EXTRACCIÓN (READ)
# ==============================
df=pd.read_csv(
    "data/hotel_bookings.csv")
print(df.head())

df.columns= df.columns.str.lower()

# ==============================
# 2. TRANSFORMACIÓN
# ==============================

# ---------PASSENGERS------------
# passengers = df[["passengers"]].drop_duplicates().reset_index(drop=True)

# passengers["passenger_id"]= passengers.index + 1

# passengers = passengers[["passenger_id","passengers"]]
# print(passengers.head())

# Hacer merge para asignar passenger_id a cada fila original
# df = df.merge(passengers, on=["passengers"], how="left")

# print(df.head())

# ---------CUSTOMER------------
customers=df[["month"]].drop_duplicates().reset_index(drop=True)
customers["customer_id"]= customers.index + 1
customers = customers[["customer_id","month"]]
print(customers.head())

df=df.merge(customers,on=["month"],how="left")
print(df.head())

# ---------Time------------
time=df[["year","month"]].drop_duplicates().reset_index(drop=True)
time["time_id"]= time.index + 1
time = time[["time_id","year","month"]]
print(time.head())

df=df.merge(time,on=["year","month"],how="left")
print(df.head())

# ---------Bookings------------
df["booking_id"] = df.index + 1
print(df.head())
booking = df[["booking_id","time_id","customer_id","passengers"]]

booking["is_high_season"] = df["month"].apply(
    lambda x: 1 if x in ["June", "July", "August"] else 0
)
print(booking.head())


# Crear conexión a PostgreSQL
engine = create_engine("postgresql://postgres:@localhost:5432/booking_db")

# Insertar datos en tablas
# append = agrega datos sin borrar lo anterior
customers.to_sql("customers", engine, if_exists="append", index=False)
time.to_sql("time", engine, if_exists="append", index=False)
booking.to_sql("booking", engine, if_exists="append", index=False)

print("Datos cargados a PostgreSQL")

# import pandas as pd
# from sqlalchemy import create_engine

# # ==============================
# # 1. EXTRACCIÓN (READ)
# # ==============================

# # Leer dataset
# df = pd.read_csv("data/hotel_bookings.csv")

# # Normalizar nombres de columnas
# df.columns = df.columns.str.lower()

# # Validar lectura
# print(df.head())


# # ==============================
# # 2. TRANSFORMACIÓN
# # ==============================

# # ---------- CUSTOMERS ----------
# # Crear dimensión de clientes (simulada por mes)
# customers = df[["month"]].drop_duplicates().reset_index(drop=True)

# # Crear ID único
# customers["customer_id"] = customers.index + 1

# # Reordenar columnas
# customers = customers[["customer_id", "month"]]

# print(customers.head())

# # Asignar customer_id al dataset original
# df = df.merge(customers, on=["month"], how="left")


# # ---------- TIME ----------
# # Crear dimensión de tiempo (year + month)
# time = df[["year", "month"]].drop_duplicates().reset_index(drop=True)

# # Crear ID único
# time["time_id"] = time.index + 1

# # Reordenar columnas
# time = time[["time_id", "year", "month"]]

# print(time.head())

# # Asignar time_id al dataset original
# df = df.merge(time, on=["year", "month"], how="left")


# # ---------- BOOKINGS (FACT TABLE) ----------
# # Crear ID único por evento
# df["booking_id"] = df.index + 1

# # Seleccionar columnas necesarias + month temporalmente
# bookings = df[[
#     "booking_id",
#     "time_id",
#     "customer_id",
#     "passengers",
#     "month"
# ]].copy()

# # Crear columna derivada (feature engineering)
# bookings["is_high_season"] = bookings["month"].apply(
#     lambda x: 1 if x in ["June", "July", "August"] else 0
# )

# # Eliminar columna auxiliar
# bookings = bookings.drop(columns=["month"])

# print(bookings.head())


# # ==============================
# # 3. VALIDACIÓN
# # ==============================

# print("Customers shape:", customers.shape)
# print("Time shape:", time.shape)
# print("Bookings shape:", bookings.shape)


# # ==============================
# # 4. LOAD (CARGA A DB)
# # ==============================

# # Conexión a PostgreSQL
# engine = create_engine("postgresql://postgres:@localhost:5432/booking_db")

# # Insertar datos (usa replace en desarrollo)
# customers.to_sql("customers", engine, if_exists="replace", index=False)
# time.to_sql("time", engine, if_exists="replace", index=False)
# bookings.to_sql("bookings", engine, if_exists="replace", index=False)

# print("Datos cargados a PostgreSQL")