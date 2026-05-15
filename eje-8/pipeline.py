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
# print(delivery_raw.shape)

print("--------------------------------------")
# delivery_raw= delivery_raw.dropna(subset=[""])

# ==============================
# SEPARACION MODELADO
# ==============================

#Table delivery_team
delivery_team= delivery_raw[["delivery_person_id"]].drop_duplicates().reset_index(drop=True)
delivery_team["team_id"]= delivery_team.index + 1
delivery_team=delivery_team[["team_id","delivery_person_id"]]
delivery_raw= delivery_raw.merge(delivery_team, on=["delivery_person_id"], how="left")

#Table city
citys= delivery_raw[["city"]].drop_duplicates().reset_index(drop=True)
citys["city_id"]= citys.index + 1
citys=citys[["city_id","city"]]
delivery_raw= delivery_raw.merge(citys, on=["city"], how="left")
print(delivery_raw.head())

#Table weather
weather= delivery_raw[["weather"]].drop_duplicates().reset_index(drop=True)
weather["weather_id"]= weather.index + 1
weather=weather[["weather_id","weather"]]
delivery_raw= delivery_raw.merge(weather, on=["weather"], how="left")

#Table order
orders= delivery_raw[["type_of_order"]].drop_duplicates().reset_index(drop=True)
orders["order_id"]= orders.index + 1
orders=orders[["order_id","type_of_order"]]
delivery_raw= delivery_raw.merge(orders, on=["type_of_order"], how="left")

#Table type vehicle
vehicle= delivery_raw[["type_of_vehicle"]].drop_duplicates().reset_index(drop=True)
vehicle["vehicle_id"]= vehicle.index + 1
vehicle=vehicle[["vehicle_id","type_of_vehicle"]]
delivery_raw= delivery_raw.merge(vehicle, on=["type_of_vehicle"], how="left")

#Table traffic
traffic= delivery_raw[["road_traffic_density"]].drop_duplicates().reset_index(drop=True)
traffic["traffic_id"]= traffic.index + 1
traffic=traffic[["traffic_id","road_traffic_density"]]
delivery_raw= delivery_raw.merge(traffic, on=["road_traffic_density"], how="left")
print(traffic.head())
print(delivery_raw.head())