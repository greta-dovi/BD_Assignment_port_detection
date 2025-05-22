import polars as pl
import math
import pathlib
import numpy as np
from sklearn.cluster import DBSCAN

# Evaluate the relative size of the port.

# Middle coordinate
# take info from all worker files and aggregate it
# Maybe radius? And then if radius overlaps - merge?


def unique_ports(df: pl.DataFrame):
    ports = df["port_unique"].explode().unique()
    unique_ports = []
    for port in ports:
        unique_ports.append(df.filter(pl.col("port_unique") == port))
    return unique_ports


def calculate_centroid(df: pl.DataFrame): # taken from stack overflow
    x, y, z = 0.0, 0.0, 0.0
    for row in df.iter_rows(named=True):
        lat = math.radians(row.get("latitude"))
        lon = math.radians(row.get("longitude"))
        
        x += math.cos(lat) * math.cos(lon)
        y += math.cos(lat) * math.sin(lon)
        z += math.sin(lat)

    total = len(df)

    x = x / total
    y = y / total
    z = z / total

    central_lon = math.atan2(y, x)
    central_square_root = math.sqrt(x * x + y * y)
    central_lat = math.atan2(z, central_square_root)

    return (math.degrees(central_lat), math.degrees(central_lon))


def merge_files():
    file_list = [file.name for file in pathlib.Path(".").glob("worker*.csv")]
    new_df = None
    
    for i, file in enumerate(file_list):
        df = pl.read_csv(file)
        df = df.with_columns(
            (pl.col("port").cast(pl.Utf8) + f"_{i}").alias("port_unique") # unique port "port_worker"
            )
        if new_df is None:
            new_df = df
        else:
            new_df = new_df.vstack(df)
    new_df.write_csv("all_workers.csv", include_header=True)



def collapse_centroids():
    centroids = []
    df = pl.read_csv("all_workers.csv")
    ports = unique_ports(df)
    for port in ports:
        if len(port) > 2:
            row = port.row(0, named=True)
            centroid = calculate_centroid(port)
            port_id = row.get("port_unique")
            mmsi_list = port["mmsi"].to_list()
            centroids.append([port_id, centroid[0], centroid[1], mmsi_list])
    d = pl.DataFrame(centroids, schema=["port", "latitude", "longitude", "mmsis_in_port"], orient="row")
    df_centroids = d.explode("mmsis_in_port")
    coords = np.radians(df_centroids.select(["latitude", "longitude"]).to_numpy())
    earth_radius = 6378.0
    db = DBSCAN(eps=5 / earth_radius, min_samples=1, metric="haversine")
    labels = db.fit_predict(coords)

    df_centroids = df_centroids.with_columns(pl.Series("port_cluster", labels))
    df_centroids.write_csv("merged_centroids.csv")


merge_files()
collapse_centroids()

