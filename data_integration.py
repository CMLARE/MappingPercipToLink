
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


# In the CML data, if the label says 00:00 then that means it contains data for next 15 minutes. we have
# calculated the attenuation for the time period 00:00 -00:15
#
# we used that convention here as well.
# if the data says 11:15 then,
# that means the sum of the readings at 11:20,11:25,11:30

# IMPORTANT : this code require the file name to be same as the weather station name used in the CML data file

# Please Uncomment as the weather stations you have

# In[4]:


def data_integration(csv_path_cml, csv_path_rainfall, integrated_file_loc):

    cml_data_frame = pd.read_csv(csv_path_cml, index_col=[2,1])

    currently_using_stns = ["Ibattara 2", "Orugodawatta", "Kottawa North Dharmapala School", "Waga"]

#     #     creating different dataframes for each weather station
#     ambewela_cml_df = cml_data_frame.loc["Ambewela"]
#     dickOya_cml_df = cml_data_frame.loc["Dick oya"]
#     hingurana_cml_df = cml_data_frame.loc["Hingurana"]
#     jaffna_cml_df = cml_data_frame.loc["Jaffna"]
#     mahapallegama_cml_df = cml_data_frame.loc["Mahapallegama"]
#     malabe_cml_df = cml_data_frame.loc["Malabe"]
#     mulleriyawa_cml_df = cml_data_frame.loc["Mulleriyawa"]
#     mutwal_cml_df = cml_data_frame.loc["Mutwal"]
    ibattara_cml_df = cml_data_frame.loc["Ibattara 2"]
    orugodawatta_cml_df = cml_data_frame.loc["Orugodawatta"]
#     thurstan_college_cml_df = cml_data_frame.loc["Thurstan College"]
#     uduwawala_cml_df = cml_data_frame.loc["Uduwawala"]
#     urumewella_cml_df = cml_data_frame.loc["Urumewella"]
    dhrmapala_scl_cml_df = cml_data_frame.loc["Kottawa North Dharmapala School"]
    waga_cml_df = cml_data_frame.loc["Waga"]


    weather_stations =['ambewela.csv', 'dick oya.csv', 'hingurana.csv', 'ibattara 2.csv', 'jaffna.csv',
                       'kottawa north dharmapala school.csv', 'mahapallegama.csv', 'malabe.csv',
                       'mulleriyawa.csv', 'mutwal.csv',
                       'orugodawatta.csv', 'thurstan college.csv', 'uduwawala.csv',
                       'urumewella.csv', 'waga.csv']


    rainfall_data_frame = pd.read_csv(csv_path_rainfall, parse_dates=["date_time"], index_col=[2,0])

    #creating different dataframes for each weather station rainfall data
#     ambewela_rainfall_df = rainfall_data_frame.loc["Ambewela"]
#     dickOya_rainfall_df = rainfall_data_frame.loc["Dick oya"]
#     hingurana_rainfall_df = rainfall_data_frame.loc["Hingurana"]
#     jaffna_rainfall_df = rainfall_data_frame.loc["Jaffna"]
#     mahapallegama_rainfall_df = rainfall_data_frame.loc["Mahapallegama"]
#     malabe_rainfall_df = rainfall_data_frame.loc["Malabe"]
#     mulleriyawa_rainfall_df = rainfall_data_frame.loc["Mulleriyawa"]
#     mutwal_rainfall_df = rainfall_data_frame.loc["Mutwal"]
    ibattara_rainfall_df = rainfall_data_frame.loc["Ibattara 2"]
    orugodawatta_rainfall_df = rainfall_data_frame.loc["Orugodawatta"]
#     thurstan_college_rainfall_df = rainfall_data_frame.loc["Thurstan College"]
#     uduwawala_rainfall_df = rainfall_data_frame.loc["Uduwawala"]
#     urumewella_rainfall_df = rainfall_data_frame.loc["Urumewella"]
    dhrmapala_scl_rainfall_df = rainfall_data_frame.loc["Kottawa North Dharmapala School"]
    waga_rainfall_df = rainfall_data_frame.loc["Waga"]

#     ambewela_rainfall_df["precipitation(mm)"] = ambewela_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     ambewela_rainfall_df = ambewela_rainfall_df.dropna()
#     ambewela_cml_df = ambewela_cml_df.merge(ambewela_rainfall_df,  left_index=True, right_index=True)


#     dickOya_rainfall_df["precipitation(mm)"] = dickOya_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     dickOya_rainfall_df = dickOya_rainfall_df.dropna()
#     dickOya_cml_df = dickOya_cml_df.merge(dickOya_rainfall_df,  left_index=True, right_index=True)

#     hingurana_rainfall_df["precipitation(mm)"] = hingurana_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     hingurana_rainfall_df = hingurana_rainfall_df.dropna()
#     hingurana_cml_df = hingurana_cml_df.merge(hingurana_rainfall_df,  left_index=True, right_index=True)

#     jaffna_rainfall_df["precipitation(mm)"] = jaffna_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     jaffna_rainfall_df = jaffna_rainfall_df.dropna()
#     jaffna_cml_df = jaffna_cml_df.merge(jaffna_rainfall_df,  left_index=True, right_index=True)

#     mahapallegama_rainfall_df["precipitation(mm)"] = mahapallegama_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     mahapallegama_rainfall_df = mahapallegama_rainfall_df.dropna()
#     mahapallegama_cml_df = mahapallegama_cml_df.merge(mahapallegama_rainfall_df,  left_index=True, right_index=True)

#     malabe_rainfall_df["precipitation(mm)"] = malabe_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     malabe_rainfall_df = malabe_rainfall_df.dropna()
#     malabe_cml_df = malabe_cml_df.merge(malabe_rainfall_df,  left_index=True, right_index=True)

#     mulleriyawa_rainfall_df["precipitation(mm)"] = mulleriyawa_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     mulleriyawa_rainfall_df = mulleriyawa_rainfall_df.dropna()
#     mulleriyawa_cml_df = mulleriyawa_cml_df.merge(mulleriyawa_rainfall_df,  left_index=True, right_index=True)

#     mutwal_rainfall_df["precipitation(mm)"] = mutwal_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     mutwal_rainfall_df = mutwal_rainfall_df.dropna()
#     mutwal_cml_df = mutwal_cml_df.merge(mutwal_rainfall_df,  left_index=True, right_index=True)

    ibattara_rainfall_df["precipitation(mm)"] = ibattara_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
    ibattara_rainfall_df = ibattara_rainfall_df.dropna()
    ibattara_cml_df = ibattara_cml_df.merge(ibattara_rainfall_df,  left_index=True, right_index=True)
    ibattara_cml_df.insert(1,"PrecipStation", "Ibattara 2")

    orugodawatta_rainfall_df["precipitation(mm)"] = orugodawatta_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
    orugodawatta_rainfall_df = orugodawatta_rainfall_df.dropna()
    orugodawatta_cml_df = orugodawatta_cml_df.merge(orugodawatta_rainfall_df,  left_index=True, right_index=True)
    orugodawatta_cml_df.insert(1,"PrecipStation", "Orugodawatta")

#     thurstan_college_rainfall_df["precipitation(mm)"] = thurstan_college_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     thurstan_college_rainfall_df = thurstan_college_rainfall_df.dropna()
#     thurstan_college_cml_df = thurstan_college_cml_df.merge(thurstan_college_rainfall_df,  left_index=True, right_index=True)

#     uduwawala_rainfall_df["precipitation(mm)"] = uduwawala_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     uduwawala_rainfall_df = uduwawala_rainfall_df.dropna()
#     uduwawala_cml_df = uduwawala_cml_df.merge(uduwawala_rainfall_df,  left_index=True, right_index=True)

#     urumewella_rainfall_df["precipitation(mm)"] = urumewella_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
#     urumewella_rainfall_df = urumewella_rainfall_df.dropna()
#     urumewella_cml_df = urumewella_cml_df.merge(urumewella_rainfall_df,  left_index=True, right_index=True)

    dhrmapala_scl_rainfall_df["precipitation(mm)"] = dhrmapala_scl_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
    dhrmapala_scl_rainfall_df = dhrmapala_scl_rainfall_df.dropna()
    dhrmapala_scl_cml_df = dhrmapala_scl_cml_df.merge(dhrmapala_scl_rainfall_df,  left_index=True, right_index=True)
    dhrmapala_scl_cml_df.insert(1,"PrecipStation", "Kottawa North Dharmapala School")

    waga_rainfall_df["precipitation(mm)"] = waga_rainfall_df["precipitation(mm)"].resample("15Min", closed="right", label="right").sum(min_count=3)
    waga_rainfall_df = waga_rainfall_df.dropna()
    waga_cml_df = waga_cml_df.merge(waga_rainfall_df,  left_index=True, right_index=True)
    dhrmapala_scl_cml_df
    waga_cml_df.insert(1,"PrecipStation", "Waga")

    all_data_frames = [ibattara_cml_df,orugodawatta_cml_df,
#                        ambewela_cml_df, dickOya_cml_df, hingurana_cml_df,
#                        jaffna_cml_df,
#                        mahapallegama_cml_df,
#                        malabe_cml_df, mulleriyawa_cml_df, mutwal_cml_df,
#                        thurstan_college_cml_df,
#                        uduwawala_cml_df, urumewella_cml_df,
                      dhrmapala_scl_cml_df,waga_cml_df]

    final_data_frame = pd.concat(all_data_frames)

#     for df in all_data_frames:
#         if not isfile(integrated_file_loc):
#            df.to_csv(integrated_file_loc)
#         else: # else it exists so append without writing the header
#            df.to_csv(integrated_file_loc, mode='a', header=False)

    final_data_frame.to_csv(integrated_file_loc, index_label= "date_time")



# In[5]:


get_all_files("/media/akalanka/Engineering/Final_Year_Project/1_DATA/CML/Proccessed/Gayan/2018-05-08 to 2018-05-15.csv",
                "/media/akalanka/Engineering/Final_Year_Project/1_DATA/RAIN/curw/",
              "/media/akalanka/Engineering/Final_Year_Project/1_DATA/RAIN/curw/rainfall_data_all.csv",
             "/media/akalanka/Engineering/Final_Year_Project/1_DATA/CML/Proccessed/Gayan/2018-05-08_to_2018-05-15_integrated.csv")


# below method is to create the classified data.
#
# in here,
#
# *  0 <  A < 0.5
# * 0.5 <= B < 1.0
# * 1.0 <= C < 2.5
# * 2.5 <= D < 5.0
# * 5.0 <= E
#
#
# * A = small rain
# * B = Average Rain
# * C = Above average
# * D = Heavy rain
# * E = Very Heavy rain

# In[30]:


def classify_data(file_path):

    classify_data_frame = pd.read_csv(file_path, index_col=[0])

#     define bins 0-0.5, 0.6-1.0, 1.1-2.5, 2.6-5.0, 5.1-infinty
    bins = [-1.0, 0.5, 1.0, 2.5, 5.0, 1000.0]

    group_names = ["A", "B", "C", "D", "E"]

    classify_data_frame["class"] = pd.cut(classify_data_frame["precipitation(mm)"], bins, labels=group_names)
    new_file = file_path[0:-4] + "_classified.csv"
    classify_data_frame.to_csv(new_file)





# In[31]:


classify_data("/media/akalanka/Engineering/Final_Year_Project/1_DATA/CML/Proccessed/Gayan/2018-05-08_to_2018-05-15_integrated.csv")

