import MySQLdb as my
db=my.connect(host="127.0.0.1",user="root",passwd="itmd521",db="itmd521")
cursor=db.cursor()
list = []
pv = open("1985.txt","r") 

#'for loop' to insert the data into 521 database 
for line in pv:
        unknown1= line[0:4] #no mention 
        wStationIdentifier = line[4:10] #US weather station identifier
        WBANIdentifier= line[10:15] #WBAN weather station identifier
        obsDate=line[15:23] #Observation date
        obsHour=line[23:27] #Observation Hour
        unknown2=line[27] #unknown
        lat=line[28:34] #latitude (degrees x 1000)
        long=line[34:41] #longitude (degrees x 1000)
        unknown3=line[41:46] #some kind of data
        elev=line[46:51] #elevation in meters
        PHF1=line[51:56] #place holder field
        PHF2=line[56:60] #place holder field
        windD=line[60:63] #wind direction
        qualityCode1=line[63] #quality code
        PH1=line[64] #place holder
        PH2=line[65:69] #place holder
	    PH3=line[69] #place holder
        PH4=line[70] # place holder
        PH5= line[70:75] #place holder for sky ceiling
        PH6= line[76] #place holder for quality code
        PH7= line[77] #place holder
        visibilityD=line[78:84] #visibility Distance
        qualityCode2=line[84] #Quality Code
        PH8=line[85] #place holder
        ph9=line[86] #place holder
        airTemperature=line[87:92] #air temperature (degree celcius X 10)
        qualityCode3=line[92] #quality code
        dewPoint=line[93:98] #dew point
        qualityCode4=line[99] #quality code
        atmosphericPressure=line[99:104] #atmospheric Pressure (hectopascals x 10)
        qualityCode5=line[105] #quality code

		#sql statement for data insertion
        sqlStatement= "insert into records VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"% \
(unknown1,wStationIdentifier,WBANIdentifier,obsDate,obsHour,unknown2,lat,long,unknown3,elev,PHF1,PHF2,windD,qualityCode1,PH1,PH2,PH3,PH4,PH5,PH6,PH7,visibilityD,qualityCode2,PH8,PH9,airTemperature,qualityCode3,dewPoint,qualityCode4,atmosphericPressure,qualityCode5)
    
	#execution of sqlStatement
	number_of_rows=cursor.execute(sqlStatement)
        
	db.commit()
    
pv.close()
db.close()

#printing message for user 
print("Data insertion has been done successfully")
