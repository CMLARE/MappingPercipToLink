
# coding: utf-8

# In[8]:


import pandas as pd
import os
from shutil import copy
import geopy.distance


# In[9]:


data_root = "/home/gayan/Desktop/CMLARE/Data"
unprocessed_folder = "2018-05-08 to 2018-05-15"
processed_folder = data_root+"/Processed/selected/"+unprocessed_folder #Output folder for each file
links_file= data_root+"/Master Files"+"/links.csv"  #List of the selected links

processed_file = data_root+"/Processed/"+unprocessed_folder + "_rainlink.csv"
rootUnprocessed_folder = data_root+"/Un Processed/"
unprocessed_folder = rootUnprocessed_folder + unprocessed_folder

test_file = unprocessed_folder+"/PM_IG30028_15_201805050000_01.csv"


# In[10]:


link_data = []
links = []
#test_file = processed_folder+"/PM_IG30028_15_201803180000_01.csv"

mapped_links_field_names = ["cell_tower_id1","cell_tower_id2","source1_name","source2_name", "district1","district2",
                            "latitude1","latitude2","longitude1","longitude2","nearest_precip_station","distance",
                            "nearest_precip_lat","nearest_precip_long","id_1","id_2","PathLength","frequency1","frequency2","frequency_band"]

link_data_field_names = ["DeviceID","DeviceName","ResourceID","ResourceName","CollectionTime","GranularityPeriod","RSL_MAX","RSL_MIN","RSL_AVG","RSL_CUR","TLHTT","TLLTT","TSL_MAX","TSL_MIN","TSL_AVG","TSL_CUR","RLHTT","RLLTT","ATPC_N_ADJUST","ATPC_P_ADJUST","ODU_SSV_TH"]

processed_data_all = []
processed_file_field_names = ["ID","Pmax","Pmin","XStart","YStart","XEnd","YEnd","DateTime","PathLength","Frequency"]


# In[11]:


directory = os.listdir(unprocessed_folder)
correct_files = []
for file in directory:
    splitted_name = file.split("_")[1]      
    if(splitted_name.endswith("28")):
        correct_files.append(file)
for file in correct_files:
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder)
    copy(src=unprocessed_folder+"/"+file,dst=processed_folder)


# In[12]:


links = pd.read_csv(links_file)


# In[13]:


def calcPathLength(processed_data):
#     print(processed_data)
    start_cord = (processed_data["XStart"],processed_data["YStart"])
    end_cord = (processed_data["XEnd"],processed_data["YEnd"])
    return geopy.distance.vincenty(start_cord, end_cord).km
def formatDate(processed_data):
    return processed_data["DateTime"].replace("-","").replace(" ","").replace(":","")[:-2]


# In[14]:


def process_file(data_file):
    global link_data
    link_data_df = pd.read_csv(data_file, skiprows=1)
    leftMerge = links.merge(link_data_df, left_on=links["source1_name"],right_on=link_data_df["ResourceName"],how="left")
    leftMerge = leftMerge.drop(columns = ["key_0"])
    rightMerge = leftMerge.merge(link_data_df, left_on=leftMerge["source2_name"],right_on=link_data_df["ResourceName"],how="left")
    rightMerge.dropna(subset=["ResourceName_x"])
    source1Data = rightMerge[["frequency_band","CollectionTime_x","RSL_MIN_x","RSL_MAX_x","latitude1","longitude1","latitude2","longitude2","Id_1"]]
    source1Data = source1Data.rename(columns={"frequency_band":"Frequency","CollectionTime_x" :"DateTime","RSL_MIN_x":"Pmin","RSL_MAX_x":"Pmax","latitude1":"XStart","longitude1":"YStart","latitude2":"XEnd","longitude2":"YEnd","Id_1":"ID"})
    source2Data = rightMerge[["frequency_band","CollectionTime_y","RSL_MIN_y","RSL_MAX_y","latitude2","longitude2","latitude1","longitude1","Id_2"]]
    source2Data = source2Data.rename(columns={"frequency_band":"Frequency","CollectionTime_y" :"DateTime","RSL_MIN_y":"Pmin","RSL_MAX_y":"Pmax","latitude2":"XStart","longitude2":"YStart","latitude1":"XEnd","longitude1":"YEnd","Id_2":"ID"})
    data = pd.concat([source1Data,source2Data])
    data = data.dropna()
    data["PathLength"] = data.apply (lambda row: calcPathLength (row),axis=1)
    data["DateTime"] = data.apply(lambda row: formatDate (row), axis=1)
    link_data = pd.concat([link_data,data])


# In[15]:


def process_all():
    global link_data
    link_data = pd.DataFrame(columns=["Frequency","DateTime","Pmin","Pmax","XStart","YStart","XEnd","YEnd","ID","PathLength"])
    directory = os.listdir(processed_folder)
    directory = list(directory)
    correct_file_paths = []
    for file in directory:
#         print(file)
        correct_file_paths.append(str(processed_folder)+"/"+file)
        process_file(str(processed_folder)+"/"+file)
        print("process Successfull")
#     print(directory)


# In[ ]:


process_all()


# In[17]:


link_data.to_csv(processed_file)

