import csv
import numpy as np
from numpy import ones,vstack
from numpy.linalg import lstsq
import math
from operator import itemgetter

cell_towers_file = "Dialog_Site_List.csv"
link_ids_file="link_ids.csv"
mapped_links_file="mapped_links.csv"
percip_station_file = "precipitaion_stations.csv"

cell_towers_field_names= ["Site ID","Suntel ID","Site Name","Site Address","District","Province","Latitude","Longitude",
                          "Latitude","Longitude"]
cell_towers = []
link_ids_field_names =["Source1","Source2"]
link_ids = []
precip_station_field_names = ["Station ID", "Country Id", "Station Name", "Latitude", "Longitude", "Elevation"]
precip_stations = []
mapped_links_field_names = ["cell_tower_id1","cell_tower_id2","source1_name","source2_name", "district1","district2",
                            "latitude1","latitude2","longitude1","longitude2","nearest_precip_station","distance",
                            "nearest_precip_lat","nearest_precip_long"]
mapped_links=[]

weather_stations_longitudes= []
weather_stations_latitudes= []

def load_files():
    global cell_towers,link_ids,weather_stations_latitudes,weather_stations_longitudes,precip_stations
    with open(cell_towers_file,"r") as cell_towers_csv:
        cell_towers = csv.DictReader(cell_towers_csv, fieldnames=cell_towers_field_names)
        cell_towers.__next__();                                                                                         #Skipping the Header
        cell_towers = list(cell_towers)
        print(" Reading Cell towers Successfull")
        with open(link_ids_file,"r") as link_ids_csv:
            link_ids = csv.DictReader(link_ids_csv,link_ids_field_names)
            link_ids.__next__()                                                                                         # Skipping the header
            link_ids = list(link_ids)
            print("reading link ids successfull")
            with open(percip_station_file, "r") as percip_station_csv:
                precip_stations = csv.DictReader(percip_station_csv, precip_station_field_names)
                precip_stations.__next__()
                precip_stations = list(precip_stations)
                print("Reading precipitaion stations was successfull")
                weather_stations_longitudes = []
                for precip_station in precip_stations:
                    weather_stations_longitudes.append(float(precip_station["Longitude"]))
                weather_stations_latitudes = []
                for precip_station in precip_stations:
                    weather_stations_latitudes.append(float(precip_station["Latitude"]))


def start_mappings():
    global cell_towers,link_ids,mapped_links
    for link_id in link_ids:
        source1_name = link_id["Source1"]
        source2_name = link_id["Source2"]
        mapped_link = {}
        mapped_link["source1_name"] = source1_name
        mapped_link["source2_name"] = source2_name
        first_set = False;
        second_set = False;
        for cell_tower in cell_towers:
            cell_tower_id = cell_tower["Site ID"]
            if(cell_tower_id in source1_name):
                first_set=True;
                mapped_link["cell_tower_id1"] = cell_tower_id
                mapped_link["district1"] = cell_tower["District"]
                mapped_link["latitude1"] = cell_tower["Latitude"]
                mapped_link["longitude1"] = cell_tower["Longitude"]
            if(cell_tower_id in source2_name):
                second_set = True
                mapped_link["cell_tower_id2"] = cell_tower_id
                mapped_link["district2"] = cell_tower["District"]
                mapped_link["latitude2"] = cell_tower["Latitude"]
                mapped_link["longitude2"] = cell_tower["Longitude"]
        if(first_set and second_set):
            mapped_links.append(mapped_link)
    for mapped_link in mapped_links:
        index_of_precip,distance=calc_distance(lat1=mapped_link["latitude1"],lang1=mapped_link["longitude1"],lat2=mapped_link["latitude2"],lang2=mapped_link["longitude2"])
        nearest_precip = precip_stations[index_of_precip]
        mapped_link["nearest_precip_station"] = nearest_precip["Station ID"]
        mapped_link["distance"] = distance
        mapped_link["nearest_precip_lat"] = nearest_precip["Latitude"]
        mapped_link["nearest_precip_long"] = nearest_precip["Longitude"]

def write_mappings():
    with open(mapped_links_file,"w") as mapped_links_csv:
        dictionary_writer = csv.DictWriter(mapped_links_csv,mapped_links_field_names)
        dictionary_writer.writeheader()
        dictionary_writer.writerows(mapped_links)

def calc_equation(lat1, lang1, lat2, lang2):
    points = [(lat1, lang1), (lat2,lang2)]
    x_coords, y_coords = zip(*points)
    A = vstack([x_coords, ones(len(x_coords))]).T
    m, c = lstsq(A, y_coords)[0]
    print("Line Solution is y = {m}x + {c}".format(m=m, c=c))
    return m,c

def calc_distance(lat1, lang1, lat2, lang2):
    m,c = calc_equation(lat1, lang1, lat2, lang2)
    long = np.array(weather_stations_longitudes)
    lat = np.array(weather_stations_latitudes)
    distance_function = lambda lat,long : abs(lat*m - long + c)/math.sqrt(lat**2 + long**2)
    vec_distance_function = np.vectorize(distance_function)
    distances = vec_distance_function(lat,long)
    index_of_minimum_distance_precip = np.argmin(distances)
    # print(distances)
    # print(index_of_minimum_distance_precip)
    return index_of_minimum_distance_precip,distances[index_of_minimum_distance_precip]

load_files()
start_mappings()
write_mappings()
