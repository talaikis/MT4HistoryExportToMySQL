import struct
import pandas as pd
import MySQLdb as mdb
import datetime
import time
from os import listdir
from os.path import isfile, join
import warnings

head_s = 148
struct_size = 60

#connect to DB
def connect_to_DB():
    
    #Connect to the <a href="http://www.talaikis.com/mysql/">MySQL</a> instance
    db_host = '127.0.0.1'
    db_user = 'root'
    db_pass = '8h^=GP655@740u9'
    db_name = 'lean'

    con = mdb.connect(host = db_host, user = db_user, passwd = db_pass, db = db_name)
    
    return con

#disconnect from databse
def disconnect(con):
    # disconnect from server
    con.close()

def create_table(tableName, con):
    str_ = """CREATE TABLE IF NOT EXISTS `"""+ tableName + """` (DATE_TIME timestamp NOT NULL, OPEN double, \
        HIGH double, LOW double, \
        CLOSE double, VOLUME double, \
        PRIMARY KEY  (DATE_TIME)) ENGINE=InnoDB AUTO_INCREMENT=0;"""
    with con:
        cursor = con.cursor()
        
        #off warnings
        warnings.filterwarnings("ignore")
        cursor.execute(str_)

#script body
if __name__ == "__main__":

    path_to_history = "C:\Users\USERNAME\AppData\Roaming\MetaQuotes\Terminal\88A7C6C356B9D73AC70BD2040F0D9829\history\Ava-Real 1\\"
    
    filenames = [f for f in listdir(path_to_history) if isfile(join(path_to_history, f))]
    
    #do it for all files
    for filename in filenames:
        try:    
            read = 0
            openTime = []
            openPrice = []
            lowPrice = []
            highPrice = []
            closePrice = []
            volume = []
            
            with open(path_to_history+filename, 'rb') as f:
                while True:
                    if read >= head_s:
                        buf = f.read(struct_size)
                        read += struct_size
                        if not buf:
                            break
                        
                        bar = struct.unpack("<Qddddqiq", buf)
                        openTime.append(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(bar[0])))
                        openPrice.append(bar[1])
                        highPrice.append(bar[2])
                        lowPrice.append(bar[3])
                        closePrice.append(bar[4])
                        volume.append(bar[5])                  
                    else:           
                        buf = f.read(head_s)
                        read += head_s
                
            data = {'0_openTime':openTime, '1_open':openPrice,'2_high':highPrice,'3_low':lowPrice,'4_close':closePrice,'5_volume':volume}
     
            result = pd.DataFrame.from_dict(data)
            result = result.set_index('0_openTime')
            result.index.name = "DATE_TIME"
            result.columns = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
            
            print "-------------------------------------------------------"
            tableName = filename[:-4]
            print tableName
            con = connect_to_DB()
            create_table(tableName, con)
            
            print "Data rows for"+tableName+": %s" %len(result)
            
            for b in range(0, len(result)):
                
                dt = ((result.index[b],
                       result.ix[b].loc['OPEN'],
                      result.ix[b].loc['HIGH'],
                      result.ix[b].loc['LOW'],
                      result.ix[b].loc['CLOSE'],
                      result.ix[b].loc['VOLUME']))
                
                str_ = """INSERT IGNORE INTO `""" +tableName+ """` (DATE_TIME, OPEN, HIGH, LOW, CLOSE, VOLUME) VALUES (\'%s\',%s,%s,%s,%s, %s);""" %dt
                
                try:
                    cursor = con.cursor()
                    #off warnings
                    warnings.filterwarnings("ignore")
                    
                    cursor.execute(str_)
                    con.commit()
                except:
                    #continue if data already in
                    continue
            disconnect(con)
            
        except:
            print "Some error"
            continue   
    
    print "All done"    