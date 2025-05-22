# Port Detection

##### Data
For this project "aisdk-2025-01-23.csv" was chosen.

##### Noise Filtering
To filter the data noise run `filter_data.py` script. <br>
In this assignment the following criteria were used to filter out noise from data:
- Speed Over Ground (SOG) must be 0 or NaN
- Latitude must be in the range of (-90, +90). However, since the data contains vessels in the European region, the values above the equator (30 degrees) were left. 
- Longitude must be in range of (-180, +180)
Data was processed in parallel manner, using built-in `multiprocessing` library. <br>
Data was also sorted according to Latitude in the descending order. <br>
This script outputs filtered and sorted .csv file. 


##### Port Detection Algorithm
To detect the marine ports run `one_port_check_port_detection.py` script. <br>
The logic behind this algorithm is as follows:
- Iterate over each row inside data; get row's coordinates and MMSI
- Check if row is row is close to any of the already detected ports (by haversine distance being less than 1 km). 
- If data point is closer than 1 km away from the port and the ship was not already recorded in this port, append data point to this port. 
- If data is further away, check the distance with the remaining ships and if they are closer than 1 km away, create a new port for them. 
- To speed up the process of checking each ship with all remaining ships, an approximation of 1 km latitude was done. Basically, all ships with latitude further away than 1km south or north were imediatelly dropped. 

This process is extremely time consuming, since with each iteration the list of ports to be checked grows rapidly. Therefore task was parallelized using `multiprocessing` library. Each worker produces a separate .csv file that will be later combined and analyzed. 

##### Port Size Evaluation and Visualization
Before evaluating the port size, the separate files produced by workers will be combined. That is because some data in separate files might be overlapping and in fact representing the same marine port. <br>
To aggregate the results run `evaluate_ports.py` script. <br>
This algorithm works as follows:
- Files from separate workers are merged into one. Each port detected in the separate files now is assigned a unique ID. 
- Then unique ports are extracted and the centroids of the coordinates inside each port are calculated. Centroid calculation formula was borrowed from stackoverflow solution. 
- Once the centroids are obtained, they are aggregated using `sklearn` library, function DBSCAN. DBSCAN produces density-based clusters and in this task the clusters were detected based on 5km haversine distance radius. 
Evaluate the relative size of the port. 
- All these steps produce a .csv file that contains the information about unique ports, their center coordinate and ships that are assigned to this port. 

Since this algorithm runs on already pre-processed and heavily reduced data, no parallelization was implemented. <br>
Port size is evaluated by running `visualize_ports.py` script. The port size is determined by the number of unique vessels that visited the port during the day. <br>
Ports were visualized using `matplotlib` and `folium` libraries. `matplotlib` solution provides a plane with the scatterplot of all ports. The size of the point represents the size of the port. `folium` solution creates an interactive .html map that allows to see the ports in their real locations and check how many unique vessels were detected in the port. The size of the point also indicates the size of the port. 

##### Limitations
Although the majority of detected instances are actually representing the ports, there are some false positives too. Visual inspection of the map shows that some vessels formed a "port" based on the matched criteria (zero or no speed, vessels at close proximity), however, the "port" appears to be in the middle of the body of water. <br>
Additionally, a trade off was made between the precision and speed. The initial version of port detection algorithm allowed to more precisely evaluate the port coordinates by assigning a vessel to a port only after checking each coordinte of the vessels, that were already assigned to a port. However, this solution expands the number of actions exponentially, hence slows down the algorithm significantly. To combat that, only the first coordinate of the port is checked to decide whether to assign vessel to a port or not. 