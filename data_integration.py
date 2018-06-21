
# coding: utf-8

# Script to integrate the rainfall data from CUrW and attenuation data from Dialog Axiata

# IMPORTANT : This script is hard coded to this specific task. it is heavily dependent on some requirements. So please make sure to follow the exact steps metioned below.

# In[1]:


import pandas as pd
from os import listdir
from os.path import isfile, join
from datetime import datetime, timedelta
import time


# IMPORTANT : please comment out the reduce_to_single_file() method if you have run it once or already have the file with all weather stations

# In[2]:


# folder_path : Path to CML data file.
# edited_file_path : Path to folder which contained station wise weather data. 
# csv_file_rainfall_data_all : Path to file all rainfall data 
# integrated_file_loc : location of the final integrated file
def get_all_files(folder_path, edited_file_path, csv_file_rainfall_data_all, integrated_file_loc):
#     Read the file names in a folder path
    onlyfiles = [f for f in listdir(edited_file_path) if (isfile(join(edited_file_path, f)) and (not f.startswith(".")))]
    print(onlyfiles)

    for i in onlyfiles:
        reduce_to_single_file(edited_file_path,i, csv_file_rainfall_data_all)
    data_integration(folder_path, csv_file_rainfall_data_all, integrated_file_loc)


# Below method reduce all the files in to a single file. 
# IMPORTATNT :   This method must run only once. otherwise it will keep appending the same file over and over. You will get an completely wrong precipitation. because if you append the same file twice you get twice the percipitation as correct one.

# In[3]:


global count
def reduce_to_single_file(edited_file_path, i, csv_file_rainfall_data_all):
    df = pd.read_csv(edited_file_path + i)
    df["PrecipStation"] = i[0:-4]
    # if file does not exist write header 
    if not isfile(csv_file_rainfall_data_all):
       df.to_csv(csv_file_rainfall_data_all, index =False)
    else: # else it exists so append without writing the header
       df.to_csv(csv_file_rainfall_data_all, index=False, mode='a', header=False)

