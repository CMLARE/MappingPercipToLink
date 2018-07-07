from datetime import datetime, timedelta
import csv

#use python 2 to run the code

file= open('input.txt.','r')
fileRead=file.read()

inputArr=fileRead.split('_')

#date array
dateArr=inputArr[0].split(',')

#precipitation array
precipitationArr=inputArr[1].split(',')

#An array to convert and save dates data as timestamps
newDateArr=[]

#conversion function(unix time to utc time)
def unix_to_utc(unix):
    return (datetime.fromtimestamp(int(unix)/1000) + timedelta(hours=0)).strftime('%Y-%m-%d %H:%M:%S')
    
for time in dateArr:
    newDateArr.append(unix_to_utc(time))

#saving the result to a csv file    
with open ('output.csv','wb') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
    for x in range(len(newDateArr)):
        filewriter.writerow([newDateArr[x],precipitationArr[x]])
    
