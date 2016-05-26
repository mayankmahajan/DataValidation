#!/usr/bin/python
from optparse import OptionParser
import commands
import re,time 
import os, sys

BLACK_FONT = ''
RED_FONT = ''
END_FONT = ''
GREEN_FONT = ''
BLUE_FONT = ''


# BLACK_FONT ='\033[01m' + '\033[81m'
# RED_FONT   ='\033[01m' + '\033[91m'
# END_FONT   ='\033[0m'
# GREEN_FONT =  '\033[01m' + '\033[92m'
# BLUE_FONT = '\033[94m'

#spark-sql> CREATE TEMPORARY FUNCTION DenseVectorUDF as 'com.guavus.densevectorudf.DenseVectorUDF'; CREATE TEMPORARY FUNCTION peakUDF as 'com.guavus.densevectorudf.PeakDenseVectorUDF'; CREATE TEMPORARY FUNCTION genericUDAF as 'com.guavus.densevectorudf.GenericDenseVectorUDAFResolver'; CREATE TEMPORARY FUNCTION averageUDF as 'com.guavus.densevectorudf.AverageDenseVectorUDF';use rc2_live_db_1; select sourcesiteid, sourcesitetypeid, sourcesiteelementid, nfnameid, destsiteid, destsitetypeid, destsiteelementtypeid, isipv6, vrfid , timestamp, DenseVectorUDF(Downlinkbyte), DenseVectorUDF(uplinkbytebuffer), DenseVectorUDF(uplinkflowbuffer),  DenseVectorUDF(downlinkflowbuffer)  , DenseVectorUDF(uplinkcostbuffer), DenseVectorUDF(downlinkcostbuffer)from (select sourcesiteid, sourcesitetypeid, sourcesiteelementid, nfnameid, destsiteid, destsitetypeid, destsiteelementtypeid, isipv6, vrfid , timestamp, genericUDAF(downlinkbytebuffer) as Downlinkbyte, genericUDAF(uplinkbytebuffer) as  upLinkByteBuffer, genericUDAF(uplinkflowbuffer) as uplinkflowbuffer,  genericUDAF(downlinkflowbuffer) as downlinkflowbuffer , genericUDAF(uplinkcostbuffer) as  uplinkcostbuffer, genericUDAF(downlinkcostbuffer) as downlinkcostbuffer from  f_nrmca_60min_3600_siteflowdatacube group by sourcesiteid, sourcesitetypeid, sourcesiteelementid, nfnameid, destsiteid, destsitetypeid, destsiteelementtypeid, isipv6, vrfid , timestamp) T;

#echo "CREATE TEMPORARY FUNCTION DenseVectorUDF as 'com.guavus.densevectorudf.DenseVectorUDF'; CREATE TEMPORARY FUNCTION peakUDF as 'com.guavus.densevectorudf.PeakDenseVectorUDF'; CREATE TEMPORARY FUNCTION genericUDAF as 'com.guavus.densevectorudf.GenericDenseVectorUDAFResolver'; CREATE TEMPORARY FUNCTION averageUDF as 'com.guavus.densevectorudf.AverageDenseVectorUDF';use rc2_p6_db; select sourcesiteid, sourcesitetypeid, sourcesiteelementid, nfnameid, destsiteid, destsitetypeid, destsiteelementtypeid, isipv6, vrfid , timestamp, DenseVectorUDF(Downlinkbyte), DenseVectorUDF(uplinkbytebuffer), DenseVectorUDF(uplinkflowbuffer),  DenseVectorUDF(downlinkflowbuffer)  , DenseVectorUDF(uplinkcostbuffer), DenseVectorUDF(downlinkcostbuffer)from (select sourcesiteid, sourcesitetypeid, sourcesiteelementid, nfnameid, destsiteid, destsitetypeid, destsiteelementtypeid, isipv6, vrfid , timestamp, genericUDAF(downlinkbytebuffer) as Downlinkbyte, genericUDAF(uplinkbytebuffer) as  upLinkByteBuffer, genericUDAF(uplinkflowbuffer) as uplinkflowbuffer,  genericUDAF(downlinkflowbuffer) as downlinkflowbuffer , genericUDAF(uplinkcostbuffer) as  uplinkcostbuffer, genericUDAF(downlinkcostbuffer) as downlinkcostbuffer from  f_nrmca_60min_3600_siteflowdatacube where timestamp<1444359600  and timestamp>=1444348800 group by sourcesiteid, sourcesitetypeid, sourcesiteelementid, nfnameid, destsiteid, destsitetypeid, destsiteelementtypeid, isipv6, vrfid , timestamp) T;"|/opt/spark/bin/spark-sql --jars /opt/tms/java/NrmcaApp.jar,/opt/tms/java/densevectorudf-nrmca3.0.jar,/opt/tms/java/attval-nrmca3.0.jar,/opt/tms/java/attvaludf-nrmca3.0.jar --master yarn --driver-memory 1G --executor-memory 3G --executor-cores 3 --num-executors 2 >/data/records
#sourcesiteid, sourcesitetypeid, sourcesiteelementid, nfnameid, destsiteid, destsitetypeid, destsiteelementtypeid, isipv6, vrfid , timestamp, DenseVectorUDF(Downlinkbyte), DenseVectorUDF(uplinkbytebuffer), DenseVectorUDF(uplinkflowbuffer),  DenseVectorUDF(downlinkflowbuffer)  , DenseVectorUDF(uplinkcostbuffer), DenseVectorUDF(downlinkcostbuffer)

joinerIndex = 0

# FH=open('records')
# SparkRecords=FH.readlines()
# FH.close()

FH=open('rec_SITE_NF_18_1_4')
SparkRecords=FH.readlines()
FH.close()

FH=open('sitetypeidmap')
siteTypeIdR=FH.readlines()
FH.close()
siteTypeIdDict={}
for siteTypeId in siteTypeIdR:
    siteTypeId,siteTypeName=siteTypeId.split(',')
    siteTypeIdDict[siteTypeId]=siteTypeName.strip('\n')

FH=open('siteidmap')
siteIdR=FH.readlines()
FH.close()
siteIdDict={}
for siteId in siteIdR:
    siteIdId,siteName=siteId.split(',')
    siteIdDict[siteIdId]=siteName.strip('\n')
    

FH=open('nfidmap')
nfIdR=FH.readlines()
FH.close()
nfIdDict={}
for nfId in nfIdR:
    nfIdId,nfName=nfId.split(',')
    nfIdDict[nfIdId]=nfName.strip('\n')


def ExponentConv(data):
    fractionPart,powerPart=data.split('E')
    powerValue=pow(10,int(powerPart))
    finalValue=float(fractionPart*powerValue)
    return finalValue
    

def MaxFunc(MaxList):
    MaxList=eval(MaxList)
    #print max(MaxList)
    #print type(max(MaxList))
    return max(MaxList)

def AvgBitRate(BitRateList,Duration):
    sumElements=0
    for elements in BitRateList:
        sumElements+=elements
    #BitRate=sumElements*8
    Avg = sumElements/Duration

def Sum(ComponentList):
    sumElements=0
    
    ComponentList=eval(ComponentList)
    for element in ComponentList:
        sumElements+=float(element)
    return sumElements

def NFScreen(DimMeaureSet,binInterval):
    totalSTSDict={}
    dimSetDict={}
    filename = 'NFScreen'
    if os.path.isfile(filename):
        os.system('rm -f '+ filename)

    for record in DimMeasureSet:
        DimSet,MeasSet=record.split(' ')

        sourcesiteid, sourcesitetypeid, sourcesiteelementid, nfnameid, isipv6, timestamp = DimSet.split('^')
        sNfKey = sourcesiteid+','+sourcesitetypeid+','+nfnameid

        dimSetDict,totalSTSDict = CreateDict(sNfKey,dimSetDict,totalSTSDict,DimSet,MeasSet,filename)
        try:
            DumpDataToFile(filename,dimSetDict,sNfKey)

        except ValueError:
            print " Exception Caught while Dumping Data ..."
            print ValueError




def SitetoNFScreen(DimMeaureSet,binInterval):
    totalSTSDict={}
    dimSetDict={}
    filename = 'SitetoNFScreen'
    if os.path.isfile(filename):
        os.system('rm -f '+ filename)

    for record in DimMeasureSet:
        DimSet,MeasSet=record.split(' ')

        sourcesiteid, sourcesitetypeid, sourcesiteelementid,nfnameid, isipv6, timestamp = DimSet.split('^')
        sNfKey = sourcesiteid+','+sourcesitetypeid+','+nfnameid

        dimSetDict,totalSTSDict = CreateDict(sNfKey,dimSetDict,totalSTSDict,DimSet,MeasSet,filename)
        try:
            DumpDataToFile(filename,dimSetDict,sNfKey)

        except ValueError:
            print " Exception Caught while Dumping Data ..."
            print ValueError











def CreateDict(key,dimSetDict,totalSTSDict,DimSet,MeasureSet,filename):
    tempDict = {}
    if filename == 'SitetoNFScreen':
        (Downlinkbyte), (uplinkbytebuffer), (uplinkflowbuffer),(downlinkflowbuffer) , (uplinkcostbuffer),(downlinkcostbuffer) = MeasureSet.split('^')
    elif filename == 'NFScreen':
         tempDict = MeasureSet.split('^')
    else:
        (Downlinkbyte), (uplinkbytebuffer), (uplinkflowbuffer),(downlinkflowbuffer) , (uplinkcostbuffer),(downlinkcostbuffer) = MeasureSet.split('^')


    if dimSetDict.has_key(key):

        dimSetDict[key]['Downlinkbyte']+=Sum(Downlinkbyte)
        dimSetDict[key]['uplinkbytebuffer']+=Sum(uplinkbytebuffer)
        dimSetDict[key]['uplinkflowbuffer']+=Sum(uplinkflowbuffer)
        dimSetDict[key]['downlinkflowbuffer']+=Sum(downlinkflowbuffer)
        dimSetDict[key]['uplinkcostbuffer']+=Sum(uplinkcostbuffer)
        dimSetDict[key]['downlinkcostbuffer']+=Sum(downlinkcostbuffer)

        dimSetDict[key]['PEAKDownlinkbyte'].append(MaxFunc(Downlinkbyte))
        dimSetDict[key]['PEAKuplinkbytebuffer'].append(MaxFunc(uplinkbytebuffer))
        dimSetDict[key]['PEAKuplinkflowbuffer'].append(MaxFunc(uplinkflowbuffer))
        dimSetDict[key]['PEAKdownlinkflowbuffer'].append(MaxFunc(downlinkflowbuffer))
        dimSetDict[key]['PEAKuplinkcostbuffer'].append(MaxFunc(uplinkcostbuffer))
        dimSetDict[key]['PEAKdownlinkcostbuffer'].append(MaxFunc(downlinkcostbuffer))

    else:
        dimSetDict[key]={}
        dimSetDict[key]['Downlinkbyte']=Sum(Downlinkbyte)
        dimSetDict[key]['uplinkbytebuffer']=Sum(uplinkbytebuffer)
        dimSetDict[key]['uplinkflowbuffer']=Sum(uplinkflowbuffer)
        dimSetDict[key]['downlinkflowbuffer']=Sum(downlinkflowbuffer)
        dimSetDict[key]['uplinkcostbuffer']=Sum(uplinkcostbuffer)
        dimSetDict[key]['downlinkcostbuffer']=Sum(downlinkcostbuffer)

        dimSetDict[key]['PEAKDownlinkbyte']=[MaxFunc(Downlinkbyte)]
        dimSetDict[key]['PEAKuplinkbytebuffer']=[MaxFunc(uplinkbytebuffer)]
        dimSetDict[key]['PEAKuplinkflowbuffer']=[MaxFunc(uplinkflowbuffer)]
        dimSetDict[key]['PEAKdownlinkflowbuffer']=[MaxFunc(downlinkflowbuffer)]
        dimSetDict[key]['PEAKuplinkcostbuffer']=[MaxFunc(uplinkcostbuffer)]
        dimSetDict[key]['PEAKdownlinkcostbuffer']=[MaxFunc(downlinkcostbuffer)]

    if totalSTSDict.has_key(DimSet):

        totalSTSDict[DimSet]['Downlinkbyte']+=Sum(Downlinkbyte)
        totalSTSDict[DimSet]['uplinkbytebuffer']+=Sum(uplinkbytebuffer)
        totalSTSDict[DimSet]['uplinkflowbuffer']+=Sum(uplinkflowbuffer)
        totalSTSDict[DimSet]['downlinkflowbuffer']+=Sum(downlinkflowbuffer)
        totalSTSDict[DimSet]['uplinkcostbuffer']+=Sum(uplinkcostbuffer)
        totalSTSDict[DimSet]['downlinkcostbuffer']+=Sum(downlinkcostbuffer)

        totalSTSDict[DimSet]['PEAKDownlinkbyte'].append(MaxFunc(Downlinkbyte))
        totalSTSDict[DimSet]['PEAKuplinkbytebuffer'].append(MaxFunc(uplinkbytebuffer))
        totalSTSDict[DimSet]['PEAKuplinkflowbuffer'].append(MaxFunc(uplinkflowbuffer))
        totalSTSDict[DimSet]['PEAKdownlinkflowbuffer'].append(MaxFunc(downlinkflowbuffer))
        totalSTSDict[DimSet]['PEAKuplinkcostbuffer'].append(MaxFunc(uplinkcostbuffer))
        totalSTSDict[DimSet]['PEAKdownlinkcostbuffer'].append(MaxFunc(downlinkcostbuffer))

    else:
        totalSTSDict[DimSet]={}

        totalSTSDict[DimSet]['Downlinkbyte']=Sum(Downlinkbyte)
        totalSTSDict[DimSet]['uplinkbytebuffer']=Sum(uplinkbytebuffer)
        totalSTSDict[DimSet]['uplinkflowbuffer']=Sum(uplinkflowbuffer)
        totalSTSDict[DimSet]['downlinkflowbuffer']=Sum(downlinkflowbuffer)
        totalSTSDict[DimSet]['uplinkcostbuffer']=Sum(uplinkcostbuffer)
        totalSTSDict[DimSet]['downlinkcostbuffer']=Sum(downlinkcostbuffer)

        totalSTSDict[DimSet]['PEAKDownlinkbyte']=[MaxFunc(Downlinkbyte)]
        totalSTSDict[DimSet]['PEAKuplinkbytebuffer']=[MaxFunc(uplinkbytebuffer)]
        totalSTSDict[DimSet]['PEAKuplinkflowbuffer']=[MaxFunc(uplinkflowbuffer)]
        totalSTSDict[DimSet]['PEAKdownlinkflowbuffer']=[MaxFunc(downlinkflowbuffer)]
        totalSTSDict[DimSet]['PEAKuplinkcostbuffer']=[MaxFunc(uplinkcostbuffer)]
        totalSTSDict[DimSet]['PEAKdownlinkcostbuffer']=[MaxFunc(downlinkcostbuffer)]

    return dimSetDict,totalSTSDict

def DumpDataToFile(filename,dimSetDict,key):
    try:
        import os
        if os.path.isfile(filename):
            FH=open(filename,'a')
        else:
            FH=open(filename,'w')
            FH.write(filename + "\n")
            FH.write("DIMENSION SET : SITE_NAME , SITE TYPE, NF NAME \n")
        nwSiteKey =key
        # for nwSiteKey in dimSetDict.keys():
        for Measure in dimSetDict[nwSiteKey].keys():
            sourcesiteid,sourcesitetypeid,nfnameid=nwSiteKey.split(',')
            if 'PEAK' in Measure:
                FH.write(RED_FONT + siteIdDict[sourcesiteid]+ '  ' + siteTypeIdDict[sourcesitetypeid] + '  ' + nfIdDict[nfnameid] + '  ' +nwSiteKey + "  :  " + Measure +"  : = " + END_FONT+ GREEN_FONT+ str(MaxFunc(str(dimSetDict[nwSiteKey][Measure]))) + END_FONT +'\n')
            else:
                FH.write(RED_FONT + siteIdDict[sourcesiteid] + '  ' + siteTypeIdDict[sourcesitetypeid] +  '  ' + nfIdDict[nfnameid]+ '  ' +nwSiteKey + "  :  " + Measure +"  : = " + END_FONT+ GREEN_FONT+ str(dimSetDict[nwSiteKey][Measure]) + END_FONT+RED_FONT  + '  AVERAGE VALUE = '+ str(dimSetDict[nwSiteKey][Measure]/binInterval) + END_FONT+'\n')
            if 'Downlinkbyte' in Measure:
                if 'PEAK' in Measure:
                    FH.write(RED_FONT + siteIdDict[sourcesiteid]+ '  ' +siteTypeIdDict[sourcesitetypeid] +  '  ' + nfIdDict[nfnameid]+ '  ' +nwSiteKey + "  :  " + "PEAK DOWNLOAD BITRATE  : = " + END_FONT+ GREEN_FONT+ str(MaxFunc(str(dimSetDict[nwSiteKey]['PEAKDownlinkbyte']*8))) + END_FONT+'\n')
                else:
                    FH.write( RED_FONT + siteIdDict[sourcesiteid]+ '  ' +siteTypeIdDict[sourcesitetypeid] + '  ' + nfIdDict[nfnameid]+ '  ' +nwSiteKey + "  :  " +"TOTAL DOWNLOAD BITRATE : = " + END_FONT+ GREEN_FONT+ str(dimSetDict[nwSiteKey]['Downlinkbyte']*8) + END_FONT+RED_FONT  + '  AVERAGE VALUE = '+ str(dimSetDict[nwSiteKey]['Downlinkbyte']*8/binInterval) + END_FONT+ '\n')
        FH.close()
    except ValueError:
        print " Exception Caught while Dumping Data ..."
        print ValueError



if __name__ == '__main__':

        parser = OptionParser(usage="usage: %prog [options] ",version="%prog 1.0",conflict_handler="resolve")
        parser.add_option("-s", "--starttime",
                        action="store",
                        dest="startTime",
                        type="str",
                        help="YYYY-MM-DD:HH:MM example:2013-01-22:00:15")
        parser.add_option("-e", "--endtime",
                        action="store",
                        dest="endTime",
                        type="str",
                        help="YYYY-MM-DD:HH:MM example:2013-01-22:00:15")
        parser.add_option("-b", "--binInterval",
                        action="store",
                        dest="binInterval",
                        type="str",
                        help="Bin Interval (secs),  example:900")

        options, args = parser.parse_args()
        if(options.startTime != None and options.endTime != None and options.binInterval != None):
                startTime = options.startTime
                endTime = options.endTime
                binInterval = options.binInterval
        else:
                print "Insufficient Arguments entered...."
                (status,output) = commands.getstatusoutput("python %s --help" %(sys.argv[0]))
                print output
                sys.exit(0)
        startTime=int(time.mktime(time.strptime(startTime, "%Y-%m-%d:%H:%M")))
        endTime=int(time.mktime(time.strptime(endTime, "%Y-%m-%d:%H:%M")))
        binInterval=endTime-startTime
        DimMeasureSet=[]
        m=re.compile('^[0-9]')

        for record in SparkRecords:
            record=record.strip('\n')

            if re.match('^[0-9]',record):
                recordList=record.split('\t')
                timestamp = int(recordList[5])

                if joinerIndex == 0:
                    for el in recordList:
                        if '[' in el:
                            joinerIndex=recordList.index(el)
                            break
                if (int(timestamp)>=startTime and int(timestamp)<endTime):
                    DimensionSet='^'.join(recordList[:joinerIndex])
                    MeasureSet='^'.join(recordList[joinerIndex:])
                    DimMeasureSet.append(DimensionSet + ' ' + MeasureSet)
        print len(DimMeasureSet)


        # SitetoSiteScreen(DimMeasureSet,binInterval)

        ##
        SitetoNFScreen(DimMeasureSet,binInterval)
        # NFScreen(DimMeasureSet,binInterval)
        print 'DONE...'