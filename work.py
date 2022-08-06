import csv
from sqlalchemy import Table, Column, Integer, String, MetaData, select, Float, create_engine

engine = create_engine('sqlite:///database.db', echo = True)

meta = MetaData()

def load_csv(csvfile):
    data = []
    with open(csvfile, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for line in reader:
            data.append(line)
        return data

measure_data = load_csv("clean_measure.csv")
station_data = load_csv("clean_stations.csv")

stations = Table(
    "stations", meta,
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)

measure = Table(
    "measure", meta,
    Column("station", String),
    Column("date", Integer),
    Column("precip", Float),
    Column("tobs", Integer),
)

ins = measure.insert().values(measure_data)
ins1 = stations.insert().values(station_data)

conn = engine.connect()
conn.execute(ins)