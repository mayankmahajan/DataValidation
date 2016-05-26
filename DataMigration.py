#!/usr/bin/python
#!/usr/bin/python

import subprocess
import re
import os
import sys
import shutil
import shlex
import time
import string
import datetime
import calendar
import json
import dateutil
import dateutil.relativedelta
import pytz
import getopt
from pytz import timezone
from datetime import datetime, timedelta


def main(argv):
   inputdb = ''
   outputdb = ''
   jarPath = ''
   agg_partitions = 64
   newXmlPath = '/opt/tms/xml_schema/NrmcaAnalyticsCubeDefinition.xml'
   startTime = 0
   endTime = 0
   outputDir = ''

   try:
      opts, args = getopt.getopt(argv,"h:j:i:o:a:x:s:e:d:")
   except getopt.GetoptError:
      print 'DataMigration.py -j <path of jar> -i <inputdb> -o <outputdb> -a <agg_partitions> -x <newXmlPath> -s <startTime> -e <endTime> -d <outputDir>'
      sys.exit(2)
   
   total = len(sys.argv)
   if total < 8:
     print 'Not enuf arguments: Try Again'
     print 'DataMigration.py -j <path of jar> -i <inputdb> -o <outputdb> -a <agg_partitions> -x <newXmlPath> -s <startTime> -e <endTime> -d <outputDir>'
     sys.exit()
 
   for opt, arg in opts:
      print arg
      if opt == '-h':
         print 'DataMigration.py -j <path of jar> -i <inputdb> -o <outputdb> -a <agg_partitions> -x <newXmlPath> -s <startTime> -e <endTime> -d <outputDir>'
         sys.exit()
      elif opt in ("-j"):
         jarPath = arg
      elif opt in ("-i"):
         inputdb = arg
      elif opt in ("-o"):
         outputdb = arg
      elif opt in ("-a"):
         agg_partitions = arg
      elif opt in ("-x"):
         newXmlPath = arg
      elif opt in ("-s"):
         startTime = arg
      elif opt in ("-e"):
         endTime = arg
      elif opt in ("-d"):
         outputDir = arg
   
   if (int(endTime) <=0 or int(startTime) <=0):
      print 'Incorrect start or end time: Try Again'
      sys.exit()


   if (inputdb == '' or outputdb == ''):
      print 'Incorrect inputdb or outputdb: Try Again'
      sys.exit()
   
   if(newXmlPath == ''):
      print 'empty newXmlPath taking default'
      sys.exit()
   
   if(jarPath == ''):
      print 'empty jarPath'
      sys.exit()

   print startTime
   print endTime
   while(int(startTime) < int(endTime)):
        outputDirFinal = outputDir+'/'+inputdb+'_'+str(startTime)
        command = ''
        command_ls = 'hadoop dfs -ls '+outputDirFinal
        status_ls = os.system(command_ls) 
        if status_ls == 0:
           command_rm = 'hadoop dfs -rmr '+outputDirFinal
           status_rmr = os.system(command_rm)
 
        command = '/opt/spark/bin/spark-submit --jars /opt/tms/java/attval-nrmca3.1.jar,/opt/tms/java/hbase-connector-nrmca3.1.jar,/opt/hive/lib/postgresql-9.3-1102-jdbc41.jar,/opt/spark/lib/datanucleus-api-jdo-3.2.6.jar,/opt/spark/lib/datanucleus-core-3.2.10.jar,/opt/spark/lib/datanucleus-rdbms-3.2.9.jar,/opt/tms/java/CubeXMLManager-nrmca3.1.jar,/opt/tms/java/insta-api-nrmca3.1.jar,/opt/oozie/lib/json-simple-1.1.jar  --class com.guavus.spark.job.DataMigratorNoPartition --master yarn --deploy-mode cluster --num-executors 14 --driver-memory 10G --executor-memory 9G --executor-cores 3 --conf spark.storage.memoryFraction=.2 '+jarPath+' '+inputdb+' ' +str(startTime)+' '+outputdb+' '+str(agg_partitions)+' '+newXmlPath+' '+outputDirFinal
        print command
        status = os.system(command)
        print status
        if status == 0:
            i = 0
            while i == 0:
               command_ls = 'hadoop dfs -ls '+outputDirFinal+'/_DONE'
               status = os.system(command_ls)
               if status != 0:
                  print 'sleeping for 60 secs'
                  time.sleep(60)
               else:
                  i = i +1
           
        if status != 0:
           print 'Migration job failed', command
           
        startTime = int(startTime) + 3600 

if __name__ == "__main__":
   main(sys.argv[1:])


