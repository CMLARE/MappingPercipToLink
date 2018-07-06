from datetime import datetime, timedelta
import csv

file= open('Ambewela.txt.','r')
fileRead=file.read()

inputArr=fileRead.split('_')

dateArr=inputArr[0].split(',')
precipitationArr=inputArr[1].split(',')

newDateArr=[]

def unix_to_utc(unix):
    return (datetime.fromtimestamp(int(unix)/1000) + timedelta(hours=0)).strftime('%Y-%m-%d %H:%M:%S')
    
for i in dateArr:
    newDateArr.append(unix_to_utc(i))

print


with open ('Waga.csv','wb') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
    for x in range(len(newDateArr)):
        filewriter.writerow([newDateArr[x],precipitationArr[x]])
    
