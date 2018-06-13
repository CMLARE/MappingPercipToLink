
# coding: utf-8

# In[26]:


#unprocessed_folder = "2018-05-08 to 2018-05-15"#folder path for the directory to be processed
unprocessed_folder = "test"
processed_folder = "/home/gayan/Desktop/CMLARE/Data/Processed/selected/"+unprocessed_folder #Output folder for each file
mapped_links_file= "/home/gayan/Desktop/CMLARE/Data/mapped_links_near_.csv"  #List of the selected links

processed_file = "/home/gayan/Desktop/CMLARE/Data/Processed/"+unprocessed_folder + ".csv"
rootUnprocessed_folder = "/home/gayan/Desktop/CMLARE/Data/Un Processed/"
unprocessed_folder = rootUnprocessed_folder + unprocessed_folder


# In[27]:


import os
from shutil import copy
import csv
import geopy.distance
from math import log
from decimal import Decimal


# In[28]:


link_data = []
mapped_links = []
#test_file = processed_folder+"/PM_IG30028_15_201803180000_01.csv"


mapped_links_field_names = ["cell_tower_id1","cell_tower_id2","source1_name","source2_name", "district1","district2",
                            "latitude1","latitude2","longitude1","longitude2","nearest_precip_station","distance",
                            "nearest_precip_lat","nearest_precip_long","id_1","id_2","PathLength"]

link_data_field_names = ["DeviceID","DeviceName","ResourceID","ResourceName","CollectionTime","GranularityPeriod","RSL_MAX","RSL_MIN","RSL_AVG","RSL_CUR","TLHTT","TLLTT","TSL_MAX","TSL_MIN","TSL_AVG","TSL_CUR","RLHTT","RLLTT","ATPC_N_ADJUST","ATPC_P_ADJUST","ODU_SSV_TH"]

processed_data_all = []
processed_file_field_names = ["ID","DateTime","PRmax","PRmin","PTmin","PTmax","PRAvg","PTAvg","PAttAvg","XStart","YStart","RSL_MIN","RSL_MAX","RSL_AVG","TSL_MIN","TSL_MAX","TSL_AVG","XEnd","YEnd","PathLength","distance","Frequency"]


# In[29]:


def dBm_to_W(dBm):
    power = 10 ** ((float(dBm)-30)/10)
    return Decimal(power).to_eng_string()


def W_to_dBm(power):
    dBm = 30 + 10 * log(float(power), 10)
    return (dBm)


# In[30]:


directory = os.listdir(unprocessed_folder)
correct_files = []
for file in directory:
    splitted_name = file.split("_")[1]      
    if(splitted_name.endswith("28")):
        print(file)
        correct_files.append(file)
for file in correct_files:
    copy(src=unprocessed_folder+"/"+file,dst=processed_folder)


# In[31]:


def start_mapping(files):
    global link_data,mapped_links_file,mapped_links_field_names,mapped_links,link_data_field_names,processed_data_all
    with open(mapped_links_file,"r") as mapped_links_csv:
        mapped_links = csv.DictReader(mapped_links_csv, fieldnames=mapped_links_field_names)
        next(mapped_links)                                                                                     #Skipping the Header
        mapped_links = list(mapped_links)
        print(" Reading Cell towers locations successfull")
        for file in files:
            with open(file,"r") as file_csv:
                print("processing file : "+file)
                link_data = csv.DictReader(file_csv,fieldnames=link_data_field_names)
                next(link_data)
                next(link_data)
                link_data = list(link_data)
                mapped_links_copy = mapped_links.copy()
                for link in link_data:
                    link_resource_name = link["ResourceName"]
                    processed_data = {}
                    processed_data["PRmin"] = dBm_to_W(link["RSL_MIN"])
                    processed_data["PRmax"] = dBm_to_W(link["RSL_MAX"])
                    processed_data["PRAvg"] = dBm_to_W(link["RSL_AVG"])
                    print(link["RSL_MAX"])
                    processed_data["PTmin"] = dBm_to_W(link["TSL_MIN"])
                    processed_data["PTmax"] = dBm_to_W(link["TSL_MAX"])
                    processed_data["PTAvg"] = dBm_to_W(link["TSL_AVG"])
                    processed_data["RSL_MIN"] = link["RSL_MIN"]
                    processed_data["RSL_MAX"] = link["RSL_MAX"]
                    processed_data["RSL_AVG"] = link["RSL_AVG"]
                    processed_data["TSL_MIN"] = link["TSL_MIN"]
                    processed_data["TSL_MAX"] = link["TSL_MAX"]
                    processed_data["TSL_AVG"] = link["TSL_AVG"]
                    processed_data["DateTime"] = link["CollectionTime"]                  #.replace("-","").replace(" ","").replace(":","")[:-2]
                    for mapped_link in mapped_links_copy:
                        mapped_link_resource_name1 = mapped_link["source1_name"]
                        mapped_link_resource_name2 = mapped_link["source2_name"]
                        if(link_resource_name == mapped_link_resource_name1):
                            processed_data["XStart"] = mapped_link["latitude2"]
                            processed_data["YStart"] = mapped_link["longitude2"]
                            processed_data["XEnd"] = mapped_link["latitude1"]
                            processed_data["YEnd"] = mapped_link["longitude1"]
                            processed_data["ID"] = mapped_link["id_1"]
                            processed_data["Frequency"] = "35"                               #TODO replace with the correct value
                            processed_data["distance"] = mapped_link["distance"]
                            start_cord = (processed_data["XStart"],processed_data["YStart"])
                            end_cord = (processed_data["XEnd"],processed_data["YEnd"])
                            processed_data["PathLength"] = geopy.distance.vincenty(start_cord, end_cord).km
                            
                            transmitted_power = processed_data["PTAvg"]
                            link2 = list(filter(lambda link: link['ResourceName'] == mapped_link_resource_name2, link_data))
#                             print(link2)
                            if(len(link2)==1):
                                link2 = link2[0]
                                RSL_AVG = link2["RSL_AVG"]
                                recieved_power = dBm_to_W(RSL_AVG)
                                PAttAvg = Decimal(transmitted_power) - Decimal(recieved_power)
                                if(PAttAvg >0):
                                    processed_data["PAttAvg"] = PAttAvg
                            mapped_links_copy.remove(mapped_link)
                            break
                        elif(link_resource_name == mapped_link_resource_name2):
                            processed_data["XStart"] = mapped_link["latitude1"]
                            processed_data["YStart"] = mapped_link["longitude1"]
                            processed_data["XEnd"] = mapped_link["latitude2"]
                            processed_data["YEnd"] = mapped_link["longitude2"]
                            processed_data["ID"] = mapped_link["id_2"]
                            processed_data["Frequency"] = "35"
                            processed_data["distance"] = mapped_link["distance"]
#                             start_cord = (processed_data["XStart"],processed_data["YStart"])
#                             end_cord = (processed_data["XEnd"],processed_data["YEnd"])
#                             processed_data["PathLength"] = geopy.distance.vincenty(start_cord, end_cord).km
                            processed_data["PathLength"] = mapped_link["PathLength"]
                            transmitted_power = processed_data["PTAvg"]
                            link2 = list(filter(lambda link: link['ResourceName'] == mapped_link_resource_name1, link_data))
                            if(len(link2)==1):
                                link2 = link2[0]
#                             print(link2)
                                RSL_AVG = link2["RSL_AVG"]
                                recieved_power = dBm_to_W(RSL_AVG)
                                PAttAvg = Decimal(transmitted_power) - Decimal(recieved_power)
                                if(PAttAvg >0):
                                    processed_data["PAttAvg"] = PAttAvg    
                            mapped_links_copy.remove(mapped_link)
                            break
                    if(("XStart" in processed_data) and ("YStart" in processed_data) and ("XEnd" in processed_data) and ("YEnd" in processed_data) and ("PAttAvg" in processed_data)):
                        processed_data_all.append(processed_data)
                print("processing file : "+file+" successfull")
                    
def process_all():
    directory = os.listdir(processed_folder)
    directory = list(directory)
    correct_file_paths = []
    for file in directory:
#         print(file)
        correct_file_paths.append(str(processed_folder)+"/"+file)
#     print(directory)
    start_mapping(correct_file_paths)
    print("process Successfull")

process_all()


# In[32]:


def write_processed_files():
    global processed_data_all
    with open(processed_file,"w") as processed_file_csv:
        dictionary_writer = csv.DictWriter(processed_file_csv,processed_file_field_names)
        dictionary_writer.writeheader()
        processed_data_all = sorted(processed_data_all , key=lambda k: k['DateTime'])
#         print(processed_data_all[0].keys())
        dictionary_writer.writerows(processed_data_all)
write_processed_files()

