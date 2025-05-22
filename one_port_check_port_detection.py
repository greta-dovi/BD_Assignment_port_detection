import polars as pl
from haversine import haversine, Unit
import multiprocessing as mp
import math
from time import time


def mmsi_in_port(port, mmsi):
    for entry in port:
        if entry[0] == mmsi:
            return True
    return False


def port_detection(vessels: pl.DataFrame, filename: str):

    num_rows, num_cols = vessels.shape
    processed_indexes = []
    ports = []

    for i in range(0, num_rows):
        row = vessels.row(i, named=True)

        if i in processed_indexes:
            processed_indexes.remove(i)
            continue
        
        lat1, lon1 = row.get("Latitude"), row.get("Longitude")
        close_coords = [(row.get("MMSI"), lat1, lon1)] # later check if more than 1 entry
        lat_distance_upper = lat1 + 1 / 111.132 # 1000 m north (sorted data from south to north)
        lat_distance_lower = lat1 - 1 / 111.132 # 1000 m south (sorted data from south to north)

        # check ports - if close to existing clusters
        skip_row = False
        for port in ports:
            skip_row = False
            distance = haversine((lat1, lon1), (port[0][1], port[0][2]), unit=Unit.METERS) # check only with first coordinate
            if distance < 1000:
                for entry in port:
                    lat2, lon2 = entry[1], entry[2]
                    if not mmsi_in_port(port, row.get("MMSI")):
                        port.append((entry[0], lat2, lon2))
                    skip_row = True
                    break
            if skip_row:
                break

        if skip_row:
            continue

        if i == num_rows:
            continue # dont check out of bounds

        checked_vessels = [] # only unique MMSIs in port
        for j in range(i + 1, num_rows):
            if j in processed_indexes:
                continue

            next_row = vessels.row(j, named=True)
            lat2, lon2 = next_row.get("Latitude"), next_row.get("Longitude")
            # when first vessel if out of latitude bounds - break search in vessels
            # but then this one doesn't add them as processed
            if lat2 > lat_distance_upper:
                break
            if lat2 < lat_distance_lower:
                continue
            if row.get("MMSI") == next_row.get("MMSI"):
                continue
            if next_row.get("MMSI") in checked_vessels:
                continue
            
            distance = haversine((lat1, lon1), (lat2, lon2), unit=Unit.METERS)
            if distance < 1000:
                close_coords.append((next_row.get("MMSI"), lat2, lon2))
                processed_indexes.append(j)
                checked_vessels.append(next_row.get("MMSI"))
        if len(close_coords) > 1:
            ports.append(close_coords)

    mmsis = []
    latitudes = []
    longitudes = []
    port_lst = []
    for index, port in enumerate(ports):
        for entry in port:
            mmsis.append(entry[0])
            latitudes.append(entry[1])
            longitudes.append(entry[2])
            port_lst.append(index)

    d = {"latitude": latitudes, "longitude": longitudes, "port": port_lst, "mmsi": mmsis}
    df = pl.DataFrame(d)
    df.write_csv(filename, include_header=True)


def do_parallel(num_workers=10):
    # df = pl.read_csv("filtered_sorted_100k.csv")
    df = pl.read_csv("filtered_sorted.csv") #.limit(1000000) for test - took ~ 46 minutes with 10 workers
    # num_workers = 10
    batch_size = math.ceil(len(df) / num_workers)
    df_list = []
    file_names = []
    inputs = []
    for i in range(0, num_workers):
        sliced = df.slice((batch_size * i), batch_size)
        df_list.append(sliced)
        file_names.append(f"worker{i}.csv")
        inputs.append((sliced, f"worker{i}.csv"))

    with mp.Pool(processes=num_workers) as pool:
        pool.starmap(port_detection, inputs)


if __name__ == "__main__":
    t1 = time()
    do_parallel(num_workers=10)
    t2 = time()
    print(t2 - t1)