
import multiprocessing as mp
import polars as pl


def read_lines(file, count)->list[str]:
    lines = []
    while len(lines) < count and (line := file.readline()):
        lines.append(line)
    return lines

def keep_columns(line_parts: list[str], ts_col_i: int, mmsi_col_i: int,  lat_col_i: int, lon_col_i: int, sog_col_i: int):
    keep = [line_parts[ts_col_i], line_parts[mmsi_col_i], line_parts[lat_col_i], line_parts[lon_col_i], line_parts[sog_col_i]]
    return ','.join(keep) + '\n'

def filter_lines(lines: list[str], ts_col_i: int, mmsi_col_i: int, lat_col_i: int, lon_col_i: int, sog_col_i: int):
    result = []
    for line in lines:
        line_parts = line.split(',')
        sog_str = line_parts[sog_col_i]
        if sog_str:
            # SOG value is not missing, check if it is 0
            sog_val = float(sog_str)
            if sog_val != 0:
                continue
        # no 'else' since missing SOG value is good
        
        lat_str = line_parts[lat_col_i]
        if not lat_str or abs(float(lat_str)) > 90 or float(lat_str) < 30: # only above equator
            continue

        lon_str = line_parts[lon_col_i]
        if not lon_str or abs(float(lon_str)) > 180:
            continue

        result.append(keep_columns(line_parts, ts_col_i, mmsi_col_i, lat_col_i, lon_col_i, sog_col_i))
    return result

def main(input, output):
    num_of_workers = 10
    lines_per_worker = 8000

    # with open('subset.csv') as fr, open('filtered.csv', 'w') as fw:
    with open(input) as fr, open(output, "w") as fw:
        with mp.Pool(processes=num_of_workers) as pool:
            # manually check first line to get column indexes
            first_line = read_lines(fr, 1)
            columns = first_line[0].split(',')
            ts_col_i = columns.index('# Timestamp')
            mmsi_col_i = columns.index('MMSI')
            lat_col_i = columns.index('Latitude')
            lon_col_i = columns.index('Longitude')
            sog_col_i = columns.index('SOG')
            fw.writelines(keep_columns(columns, ts_col_i, mmsi_col_i, lat_col_i, lon_col_i, sog_col_i)) 

            lines_to_read = lines_per_worker * num_of_workers
            while lines := read_lines(fr, lines_to_read):
                batches = [(lines[i * lines_per_worker:(i+1) * lines_per_worker], ts_col_i, mmsi_col_i, lat_col_i, lon_col_i, sog_col_i)   for i in range(num_of_workers)]
                filtered_lines = pool.starmap(filter_lines, batches)
                for batch in filtered_lines:
                    fw.writelines(batch)                

# input = "subset.csv"
# input = "subset_100k.csv"
# output = "filtered_sorted_100k.csv"
input = "aisdk-2025-01-23.csv"
output = "filtered_sorted.csv"

def sort_latitude():
    df = pl.read_csv(output)
    df_sorted = df.sort("Latitude")
    df_sorted.write_csv(output, include_header=True)


if __name__ == "__main__":
    main(input, output)
    sort_latitude()