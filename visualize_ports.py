from evaluate_ports import calculate_centroid
import polars as pl
import pandas as pd
import matplotlib.pyplot as plt
import folium

# Visualize ports. (Get creatively to it). 
# Good to know the location of the port, the relative size of the port and etc.

def port_size(df: pl.DataFrame):
    ports = df["port_cluster"].explode().unique()
    vessels_in_port = []
    for port in ports:
        sub = df.filter(pl.col("port_cluster") == port)
        unique_vessels = sub["mmsis_in_port"].explode().unique()
        new_centroid = calculate_centroid(sub)
        vessels_in_port.append([port, len(unique_vessels), new_centroid[0], new_centroid[1]])
    return vessels_in_port


df_centroids = pl.read_csv("merged_centroids.csv")
d = pd.DataFrame(port_size(df_centroids), columns=("port", "number_of_vessels", "latitude", "longitude")) # leaving pandas for easier visualization


plt.figure(figsize=(8, 6))
scatter = plt.scatter(
    d["longitude"],
    d["latitude"],
    s=d["number_of_vessels"] * 10, 
    alpha=0.6,
    c=d["number_of_vessels"], 
    cmap="viridis",
    edgecolors="black",
)

plt.colorbar(scatter, label="Number of vessels")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Ports by Number of Vessels")
plt.grid(True)
plt.show()


# Interactive map
# Center map on average location
center_lat = d["latitude"].mean()
center_lon = d["longitude"].mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=7)

for _, row in d.iterrows():
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=row["number_of_vessels"] / 2, 
        popup=f"Port: {row['port']}\nVessels: {row['number_of_vessels']}",
        color="blue",
        fill=True,
        fill_opacity=0.6,
    ).add_to(m)


m.save("ports_map.html")