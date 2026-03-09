import pandas as pd
# extract data
# df= pd.read_csv("data/titanic.csv")
# print(df.head())
# print(df.info())

#Transform data
df = pd.read_csv("data/titanic.csv")
df.columns = df.columns.str.lower()

# eliminar columnas innecesarias
df = df.drop(columns=["passengerid", "name", "ticket", "cabin"])

# rellenar edades vacías
df["age"] = df["age"].fillna(df["age"].mean())

# rellenar puerto de embarque
df["embarked"] = df["embarked"].fillna(df["embarked"].mode()[0])

# crear columna is_child
df["is_child"] = df["age"].apply(lambda x: 1 if x < 18 else 0)

print(df.head())

from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:@localhost:5432/titanic_db")

df.to_sql("passengers", engine, if_exists="replace", index=False)

print("Datos cargados a PostgreSQL")

# israelcastanedaluna@MacBook-Air-de-Israel ~ % psql postgres
# psql (14.17 (Homebrew))
# Type "help" for help.

# postgres=# CREATAE DATABASE titanic_db;
# ERROR:  syntax error at or near "CREATAE"
# LINE 1: CREATAE DATABASE titanic_db;
#         ^
# postgres=# CREATE DATABASE titanic_db;
# CREATE DATABASE
# postgres=# \c titanic_db
# You are now connected to database "titanic_db" as user "israelcastanedaluna".
# titanic_db=# CREATE TABLE passengers (
#     survived INT,
#     pclass INT,
#     sex VARCHAR(10),
#     age FLOAT,
#     sibsp INT,
#     parch INT,
#     fare FLOAT,
#     embarked VARCHAR(10),
#     is_child INT
# );
# CREATE TABLE
# titanic_db=# \q