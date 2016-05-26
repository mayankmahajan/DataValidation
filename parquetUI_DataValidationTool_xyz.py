#!/usr/bin/python
from optparse import OptionParser
import commands
import re,time 
import os, sys
from datetime import datetime
from dateutil import tz

# need to integrate the parquet queries

# cstTZ = tz.gettz('CST6CDT')
cstTZ = tz.gettz('GMT')


columns_f_nrmca_60min_3600_nfcube = ['timestamp','nfnameid','isipv6','bytebuffer','flowbuffer','costbuffer']
columns_f_nrmca_60min_3600_sitedatacube = ['sourcesiteid', 'sourcesitetypeid', 'sourcesiteelementid', 'sourcesiteelementtypeid', 'nfnameid', 'isipv6', 'timestamp','downlinkbytebuffer','uplinkbytebuffer','uplinkflowbuffer','downlinkflowbuffer','uplinkcostbuffer','downlinkcostbuffer']
columns_f_nrmca_60min_3600_siteflowdatacube = ['sourcesiteid', 'sourcesitetypeid', 'sourcesiteelementid', 'sourcesiteelementtypeid', 'nfnameid', 'destsiteid', 'destsitetypeid', 'destsiteelementid', 'destsiteelementtypeid', 'isipv6', 'vrfid', 'timestamp','downlinkbytebuffer','uplinkbytebuffer','uplinkflowbuffer','downlinkflowbuffer','uplinkcostbuffer','downlinkcostbuffer','transitbuffer']

uplinkColsForCSV =      {   'AGGR' : ['AGGR_uplinkbitbuffer','AGGR_uplinkbytebuffer','AGGR_uplinkflowbuffer','uplinkcostbuffer'],
                            'PEAK' : ['PEAK_uplinkbitbuffer','PEAK_uplinkbytebuffer','PEAK_uplinkflowbuffer','uplinkcostbuffer']    }
downlinkColsForCSV =    {   'AGGR' : ['AGGR_downlinkbitbuffer','AGGR_downlinkbytebuffer','AGGR_downlinkflowbuffer','downlinkcostbuffer'],
                            'PEAK' : ['PEAK_downlinkbitbuffer','PEAK_downlinkbytebuffer','PEAK_downlinkflowbuffer','downlinkcostbuffer']    }
upDownlinkColsForCSV =  {   'AGGR' : ['AGGR_totalbitbuffer','AGGR_totalByteBuffer','AGGR_totalflowbuffer','totalcostbuffer','AGGR_uplinkbytebuffer','AGGR_downlinkbytebuffer'],
                            'PEAK' : ['PEAK_totalbitbuffer','PEAK_totalByteBuffer','PEAK_totalflowbuffer','totalcostbuffer','PEAK_uplinkbytebuffer','PEAK_downlinkbytebuffer']    }
transitDownlinkForCSV =    {   'AGGR' : ['sourcesiteid','destsiteid','AGGR_downlinkbytebuffer'],
                                'PEAK' : ['sourcesiteid','destsiteid','PEAK_downlinkbytebuffer']    }
trendsCSV =             {   'AGGR' : ['AGGR_totalbitbuffer','AGGR_totalByteBuffer','AGGR_downlinkflowbuffer','AGGR_uplinkflowbuffer','AGGR_totalflowbuffer','totalcostbuffer','AGGR_uplinkbytebuffer','AGGR_downlinkbytebuffer'],
                            'PEAK' : ['PEAK_totalbitbuffer','PEAK_totalByteBuffer','PEAK_downlinkflowbuffer','PEAK_uplinkflowbuffer','PEAK_totalflowbuffer','totalcostbuffer','PEAK_uplinkbytebuffer','PEAK_downlinkbytebuffer']    }

# FH=open('rec_f_nrmca_60min_3600_sitedatacube_1451192400')      #Data for Dec27 05:00
# FH=open('rec_f_nrmca_60min_3600_sitedatacube_1451329200')      #Data for Dec28 19:00
# FH=open('rec_f_nrmca_60min_3600_sitedatacube_1451368800_daily')
# FH=open('rec_SITE_NF')
# FH=open('/Users/mayank.mahajan/rec_f_nrmca_60min_3600_sitedatacube_26Dec_21_24_CST')
# FH=open('/Users/mayank.mahajan/rec_f_nrmca_60min_3600_sitedatacube_26Dec_00_24_CST')

FH=open('/Users/mayank.mahajan/neerav_sitedatacube_nrmcad31p1')
rawData_f_nrmca_60min_3600_sitedatacube=FH.readlines()
# FH.close()neerav_sitedatacube_qasprintnrmca

# FH=open('rec_f_nrmca_60min_3600_siteflowdatacube_1451192400')      #Data for Dec27 05:00
# FH=open('rec_f_nrmca_60min_3600_siteflowdatacube_1451192400_18_1_1')
# FH=open('f_nrmca_60min_3600_siteflowdatacube_1451329200')           #Data for Dec28 19:00
# FH=open('rec_f_nrmca_60min_3600_siteflowdatacube_1451192400_TransitSites')       #Data for Dec27 05:00   Transit Sites

# # FH=open('testData1')       #Data for Dec27 05:00   Transit Sites
# FH=open('/Users/mayank.mahajan/rec_f_nrmca_60min_3600_siteflowdatacube_26Dec_21_24_CST')
# FH=open('/Users/mayank.mahajan/rec_f_nrmca_60min_3600_siteflowdatacube_26Dec_00_24_CST')
# FH=open('/Users/mayank.mahajan/rec_f_nrmca_60min_3600_siteflowdatacube_24Dec_0500_CST')
FH=open('/Users/mayank.mahajan/neerav_siteflowdatacube_nrmcad31p1')
rawData_f_nrmca_60min_3600_siteflowdatacube=FH.readlines()
FH.close()

# FH=open('rec_f_nrmca_60min_3600_nfcube_1451192400')      #Data for Dec27 05:00
# FH=open('rec_f_nrmca_60min_3600_nfcube_1451329200')      #Data for Dec28 19:00
# FH=open('/Users/mayank.mahajan/rec_f_nrmca_60min_3600_nfcube_26Dec_21_24_CST')
# rawData_f_nrmca_60min_3600_nfcube=FH.readlines()
# FH.close()

FH=open('/Users/mayank.mahajan/sitetypeidmap')
siteTypeIdR=FH.readlines()
FH.close()
siteTypeIdDict={}
for siteTypeId in siteTypeIdR:
    siteTypeId,siteTypeName=siteTypeId.split(',')
    siteTypeIdDict[siteTypeId]=siteTypeName.strip('\n')

FH=open('/Users/mayank.mahajan/siteidmap')
siteIdR=FH.readlines()
FH.close()
siteIdDict={}
for siteId in siteIdR:
    siteIdId,siteName=siteId.split(',')
    siteIdDict[siteIdId]=siteName.strip('\n')
    

FH=open('/Users/mayank.mahajan/nfidmap')
nfIdR=FH.readlines()
FH.close()
nfIdDict={}
for nfId in nfIdR:
    nfIdId,nfName=nfId.split(',')
    nfIdDict[nfIdId]=nfName.strip('\n')



FH=open('/Users/mayank.mahajan/elementtypeidmap')
elTypeIdR=FH.readlines()
FH.close()
elTypeIdDict={}
for elTypeId in elTypeIdR:
    elTypeId,elTypeName=elTypeId.split(',')
    elTypeIdDict[elTypeId]=elTypeName.strip('\n')




def MaxFunc(key,MaxList):
    try:
        # MaxList=eval(MaxList)
        return max(MaxList)
    except:
        return GetNameFromId(key,MaxList)

def GetNameFromId(key,data):
    if key == 'nfnameid':
        return nfIdDict[str(data)]
    elif key == 'sourcesiteelementtypeid' or key == 'destsiteelementtypeid':
        return elTypeIdDict[str(data)]
    elif key == 'sourcesiteid' or key == 'destsiteid' or key == 'transitsiteid':
        try:
            return siteIdDict[str(data)] if str(data) in siteIdDict else str(data)
        except:
            print data
    elif key == 'sourcesitetypeid' or key == 'destsitetypeid':
        return siteTypeIdDict[str(data)]
    elif key == 'sourcesiteelementid' or key == 'destsiteelementid' or key == 'isipv6' :
        return data
    elif key == 'timestamp':
        data = int(data)
        # return str(data) # added for temp
        date = datetime.utcfromtimestamp(data).replace(tzinfo=tz.gettz('UTC')).astimezone(cstTZ)
        return str(date.strftime("%b %d %a %Y %H:%M %Z"))
    else:
        return data[0] if (type(data) is list and len(data) == 1 ) else data if (data != '') else ''

def Sum(key,ComponentList):
    sumElements=0
    try:
        # ComponentList=eval(ComponentList)
        for element in ComponentList:
            sumElements+=float(element)
        return sumElements
    except:
        return GetNameFromId(key,ComponentList)

def openFileToWrite(dimName,prevdimSelected,filename):
    try:
        os.mkdir(str(startExecutionTime) + dimName + '_' + prevdimSelected)
        # os.mkdir(dimName + '_' + prevdimSelected)
    except:
        pass
    filename = str(startExecutionTime)+dimName + '_' + prevdimSelected + '/' + filename
    if os.path.isfile(filename):
        os.system('rm -f '+ filename)
    return open(filename,'w')

def getProcessedData(rawData,columns,binInterval):
    processedDataPerRecord = {}
    processedData = []
    for record in rawData:
        if re.match('^[0-9]',record):
            record = record.strip('\n')
            recordArray =  re.split('\t',record)
            processedDataPerRecord = dict(zip(columns,recordArray))

            if 'uplinkbytebuffer' in processedDataPerRecord.keys():
                processedDataPerRecord['PEAK_uplinkbitbuffer'] = (MaxFunc('uplinkbytebuffer', eval(processedDataPerRecord['uplinkbytebuffer'])) * 8) /300
                processedDataPerRecord['AGGR_uplinkbitbuffer'] = (Sum('uplinkbytebuffer', eval(processedDataPerRecord['uplinkbytebuffer'])) * 8) /3600
                processedDataPerRecord['PEAK_downlinkbitbuffer'] = (MaxFunc('downlinkbytebuffer', eval(processedDataPerRecord['downlinkbytebuffer'])) * 8) /300
                processedDataPerRecord['PEAK_downlinkbitbuffer'] = (MaxFunc('downlinkbytebuffer', eval(processedDataPerRecord['downlinkbytebuffer'])) * 8) /300
                processedDataPerRecord['AGGR_downlinkbitbuffer'] = (Sum('downlinkbytebuffer', eval(processedDataPerRecord['downlinkbytebuffer'])) * 8) /3600
                processedDataPerRecord['PEAK_totalbitbuffer'] = processedDataPerRecord['PEAK_uplinkbitbuffer'] + processedDataPerRecord['PEAK_downlinkbitbuffer']
                processedDataPerRecord['AGGR_totalbitbuffer'] = processedDataPerRecord['AGGR_uplinkbitbuffer'] + processedDataPerRecord['AGGR_downlinkbitbuffer']

            if 'transitbuffer' in processedDataPerRecord.keys() and 'FFFFFFFFFFFFFFFF' in processedDataPerRecord['transitbuffer']:
                processedDataPerRecord['transitbuffer'] = '';
            if 'transitbuffer' in processedDataPerRecord.keys() and processedDataPerRecord['transitbuffer'] != '' and 'FFFFFFFFFFFFFFFF' not in processedDataPerRecord['transitbuffer']:

                processedDataPerRecord['transitsiteid'] = []
                processedDataPerRecord['transitsitetypeid'] = []
                processedDataPerRecord['transitsite'] = []

                if len(processedDataPerRecord['transitbuffer']) >16:
                    pass

                if len(processedDataPerRecord['transitbuffer']) >=16:
                    totalTransitSites = len(processedDataPerRecord['transitbuffer'])/16
                    for i in range(0,totalTransitSites):
                        transitKey = processedDataPerRecord['transitbuffer'][i*16:i*16+16]
                        eachTransitsite = int(transitKey[:8],16)
                        eachTransitsiteId = int(transitKey[8:],16)
                        processedDataPerRecord['transitsiteid'].append(eachTransitsite)
                        processedDataPerRecord['transitsitetypeid'].append(eachTransitsiteId)
                        processedDataPerRecord['transitsite'].append(GetNameFromId('transitsiteid',eachTransitsite) + ';' + str(eachTransitsiteId))
            for eachKey in processedDataPerRecord.keys():
                try:
                    processedDataPerRecord[eachKey] = eval(processedDataPerRecord[eachKey])
                except:
                    pass
                if type(processedDataPerRecord[eachKey]) is list:
                    if len(processedDataPerRecord[eachKey])>1 :
                        processedDataPerRecord['AGGR_'+eachKey] = Sum('AGGR'+eachKey,processedDataPerRecord[eachKey])
                        processedDataPerRecord['PEAK_'+eachKey] = MaxFunc('Max'+eachKey,processedDataPerRecord[eachKey])
                    else:
                        processedDataPerRecord[eachKey] = processedDataPerRecord[eachKey][0]


            processedDataPerRecord['AGGR_totalByteBuffer'] = processedDataPerRecord['AGGR_uplinkbytebuffer'] + processedDataPerRecord['AGGR_downlinkbytebuffer']
            processedDataPerRecord['PEAK_totalByteBuffer'] = processedDataPerRecord['PEAK_uplinkbytebuffer'] + processedDataPerRecord['PEAK_downlinkbytebuffer']
            processedDataPerRecord['AGGR_totalflowbuffer'] = processedDataPerRecord['AGGR_uplinkflowbuffer'] + processedDataPerRecord['AGGR_downlinkflowbuffer']
            processedDataPerRecord['PEAK_totalflowbuffer'] = processedDataPerRecord['PEAK_uplinkflowbuffer'] + processedDataPerRecord['PEAK_downlinkflowbuffer']
            # processedDataPerRecord['uplinkcostbuffer'] = processedDataPerRecord['uplinkcostbuffer'][0]
            # processedDataPerRecord['downlinkcostbuffer'] = processedDataPerRecord['downlinkcostbuffer'][0]
            processedDataPerRecord['totalcostbuffer'] = processedDataPerRecord['uplinkcostbuffer'] + processedDataPerRecord['downlinkcostbuffer']
            if 'transitsite' not in processedDataPerRecord.keys():
                processedDataPerRecord['transitsiteid'] = []
                processedDataPerRecord['transitsitetypeid'] = []
                processedDataPerRecord['transitsite'] = []


            processedData.append(processedDataPerRecord)
    return processedData


def filterDataByTime(startTime,endTime,processedData):
    pD = []
    for eachRecord in processedData:
        if eachRecord['timestamp'] >= startTime and eachRecord['timestamp'] < endTime:
            pD.append(eachRecord)
    return pD
def applyFilters(processedData,dimName,prevdimSelected):
    newPD = []

    if prevdimSelected != '':
        for eachRecord in processedData:
            arrayOfFilters = re.split(';',prevdimSelected)
            counter = 0
            for el in arrayOfFilters:
                str1 = [re.split('=',el)][0][0]
                val1 = int([re.split('=',el)][0][1])

                if eachRecord[str1] != val1:
                    counter +=1
            if counter < 1 :
                newPD.append(eachRecord)
        return  newPD
    else:
        return processedData


def getProcessedDataWithUniqueDim(newPD,dimName,prevdimSelected):
    finalPD = []
    # newPD = []
    # for eachRecord in processedData:
    #     arrayOfFilters = re.split(';',prevdimSelected)
    #     counter = 0
    #     for el in arrayOfFilters:
    #         if eachRecord[[re.split('=',el)][0][0]] != int([re.split('=',el)][0][1]):
    #             counter +=1
    #     if counter < 1 :
    #         newPD.append(eachRecord)
    if 'timestamp' in dimName:
        dimName = 'timestamp';


    newPD = sorted(newPD, key=lambda k: k['timestamp'])
    newPD = sorted(newPD, key=lambda k: k[dimName])
    # newPD = sorted(newPD, key=lambda k: k['timestamp'])
    for eachRecord in newPD:
        if len(finalPD) < 1:
            finalPD.append(eachRecord)
        else:
            # if finalPD[len(finalPD)-1][dimName] == eachRecord[dimName] and finalPD[len(finalPD)-1]['timestamp'] == eachRecord['timestamp'] :
            if finalPD[len(finalPD)-1][dimName] == eachRecord[dimName]:
                for key in eachRecord.keys():
                    if 'id' in key or 'ip' in key or 'time' in key:
                        pass
                    else:
                        try:
                            finalPD[len(finalPD)-1][key] +=eachRecord[key]
                        except:
                            pass
            else:
                finalPD.append(eachRecord)
    return finalPD

def whatToWrite(processedData,dimName,prevdimSelected,columnsForCSV,agg_peak):
    # filecounter+= 1;
    import random

    if (type(columnsForCSV) is list):
        if 'down' in columnsForCSV[0]:
            fname='downlink'
        elif 'uplink' in columnsForCSV[0]:
            fname='uplink'
        elif 'total' in columnsForCSV[0]:
            fname='total'
        else:
            fname='uplink'
    else:
        fname = columnsForCSV

    FH = openFileToWrite(dimName,prevdimSelected,agg_peak + '_' + fname + '_' + str(time.time()) + str(random.randint(0,99999999999)) + '.csv' )
    # FH = openFileToWrite(dimName,prevdimSelected,columnsForCSV[0] + '_' + columnsForCSV[1] + '_' + str(time.time()) + '.csv' )
    cols = []

    if 'timestamp' not in dimName:
        cols.append(dimName)
    else:
        cols.append(dimName.split(';')[1])
        cols.append('timestamp')
    # cols.append('timestamp')
    # cols.append(dimName)

    if (type(columnsForCSV) is list):
        cols+=columnsForCSV
    else:
        cols.append(columnsForCSV)


    # cols+=columnsForCSV
    result =''
    totalDict = {}

    for eachRecord in processedData:

        # Creating Total Row for CSV
        for key in cols:
            if key not in dimName and 'id' not in key and 'timestamp' not in key :
                if key in totalDict:
                    totalDict[key] += GetNameFromId(key,eachRecord[key])
                else:
                    totalDict[key] = GetNameFromId(key,eachRecord[key])

    total = ''
    count = 0
    header = ''
    for eachRecord in processedData:

        if prevdimSelected != '':
            arrayOfFilters = re.split(';',prevdimSelected)
            counter = 0
            for el in arrayOfFilters:
                if eachRecord[[re.split('=',el)][0][0]] != int([re.split('=',el)][0][1]):
                    counter +=1
            if counter > 0 :
                continue

        # if 'transitsite' not in cols:
        for key in cols:
            if key not in dimName and 'timestamp' not in cols and 'id' not in key:
                result +=  str((GetNameFromId(key,eachRecord[key])/totalDict[key])*100) + ","
                total+= '100,'
                if count == 0:
                    header += key + '(%),'

            else:
                result +=  str(GetNameFromId(key,eachRecord[key])) + ","
                if count == 0:
                    header += key + ','
                if 'timestamp' not in dimName:
                    total = 'Total,'
        for key in cols:
            if key not in dimName and 'timestamp' not in cols and 'id' not in key:
                result +=  str(GetNameFromId(key,eachRecord[key])) + ","
                total+= str(totalDict[key]) + ','
                if count == 0:
                    header += key + ','
        if count ==0:
            header+='\n'
        count +=1
        result+='\n'
    result = result.replace(',\n','\n')
    header = header.replace(',\n','\n')
    FH.write(header)
    FH.write(result)
    FH.write(total[:-1])
    FH.close()


def writeToCSVFile(processedData,dimName,sortField,columns,prevdimSelected):
    processedData = sorted(processedData, key=lambda k: k[sortField],reverse=True)
    if 'transit' in dimName:
        whatToWrite(processedData,dimName,prevdimSelected,transitDownlinkForCSV['AGGR'],'AGGR')
        whatToWrite(processedData,dimName,prevdimSelected,transitDownlinkForCSV['PEAK'],'PEAK')
    elif 'timestamp' in dimName:
        for measure in trendsCSV['AGGR']:
            whatToWrite(processedData,dimName,prevdimSelected,measure,'AGGR')
        for measure in trendsCSV['PEAK']:
            whatToWrite(processedData,dimName,prevdimSelected,measure,'PEAK')

        # whatToWrite(processedData,dimName,prevdimSelected,trendsCSV['PEAK'],'PEAK')
    else:
        whatToWrite(processedData,dimName,prevdimSelected,uplinkColsForCSV['AGGR'],'AGGR')
        whatToWrite(processedData,dimName,prevdimSelected,uplinkColsForCSV['PEAK'],'PEAK')
        whatToWrite(processedData,dimName,prevdimSelected,downlinkColsForCSV['AGGR'],'AGGR')
        whatToWrite(processedData,dimName,prevdimSelected,downlinkColsForCSV['PEAK'],'PEAK')
        whatToWrite(processedData,dimName,prevdimSelected,upDownlinkColsForCSV['AGGR'],'AGGR')
        whatToWrite(processedData,dimName,prevdimSelected,upDownlinkColsForCSV['PEAK'],'PEAK')

def createScreenDataFile(dimName,prevdimSelected,rawData,columns,sortField,binInterval):
    processedData = getProcessedData(rawData,columns,binInterval)
    processedData = filterDataByTime(startTime,endTime,processedData)
    processedData = applyFilters(processedData,dimName,prevdimSelected)
    if dimName != 'transitsite':
        processedDataWithUniqueDimension = getProcessedDataWithUniqueDim(processedData,dimName,prevdimSelected)
    else:
        processedDataWithUniqueDimension = processedData
    writeToCSVFile(processedDataWithUniqueDimension,dimName,sortField,columns,prevdimSelected)


if __name__ == '__main__':
        startExecutionTime = time.time()

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

        # filecounter = 0
        # Below to get CSVs for each Screens and Widgets

        # SITE TO SITE-SITE TO NF
        # createScreenDataFile('nfnameid','destsitetypeid=1;sourcesitetypeid=1;sourcesiteid=18;destsiteid=27',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'PEAK_downlinkbytebuffer',binInterval)

        # SITE
        createScreenDataFile('sourcesiteid','sourcesitetypeid=1',rawData_f_nrmca_60min_3600_sitedatacube,columns_f_nrmca_60min_3600_sitedatacube,'AGGR_uplinkbytebuffer',binInterval)

        # SITE TO NF
        # createScreenDataFile('nfnameid','sourcesiteid=18;sourcesitetypeid=1',rawData_f_nrmca_60min_3600_sitedatacube,columns_f_nrmca_60min_3600_sitedatacube,'PEAK_uplinkbitbuffer',binInterval)

        # NF TO SITE
        # createScreenDataFile('sourcesiteid','nfnameid=5;sourcesitetypeid=1',rawData_f_nrmca_60min_3600_sitedatacube,columns_f_nrmca_60min_3600_sitedatacube,'PEAK_uplinkbitbuffer',binInterval)

        # SITE TO SITE-SITE
        # createScreenDataFile('destsiteid','destsitetypeid=1;sourcesitetypeid=1;sourcesiteid=18',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'AGGR_downlinkbitbuffer',binInterval)


        # SITE TO NE
        # createScreenDataFile('sourcesiteelementtypeid','sourcesitetypeid=1;sourcesiteid=3582',rawData_f_nrmca_60min_3600_sitedatacube,columns_f_nrmca_60min_3600_sitedatacube,'AGGR_downlinkbytebuffer',binInterval)

        # SITE TO NE TO QTRENDS
        # createScreenDataFile('timestamp;sourcesiteelementtypeid','sourcesiteelementtypeid=39;sourcesitetypeid=1;sourcesiteid=3582',rawData_f_nrmca_60min_3600_sitedatacube,columns_f_nrmca_60min_3600_sitedatacube,'timestamp',binInterval)

        # SITE TO NE-NE  --- in progress
        # createScreenDataFile('destsiteelementtypeid','sourcesitetypeid=1;sourcesiteid=22;sourcesiteelementtypeid=18',rawData_f_nrmca_60min_3600_sitedatacube,columns_f_nrmca_60min_3600_sitedatacube,'AGGR_downlinkbytebuffer',binInterval)


        # SITE TO SITE-SITE TO NE
        # createScreenDataFile('sourcesiteelementtypeid','sourcesitetypeid=1;sourcesiteid=3582;destsiteid=3581;destsitetypeid=1',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'PEAK_downlinkbytebuffer',binInterval)
        # SITE TO SITE-SITE TO NE to QTrends
        # createScreenDataFile('timestamp;sourcesiteelementtypeid','sourcesiteelementtypeid=15;sourcesitetypeid=1;sourcesiteid=22;destsiteid=14;destsitetypeid=1',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'timestamp',binInterval)

        # SITE TO SITE-SITE TO NE-NE
        # createScreenDataFile('destsiteelementtypeid','sourcesiteelementtypeid=39;sourcesitetypeid=1;sourcesiteid=3582;destsiteid=3581;destsitetypeid=1',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'AGGR_downlinkbytebuffer',binInterval)
        # SITE TO SITE-SITE TO NE-NE to QTrends
        # createScreenDataFile('timestamp;destsiteelementtypeid','destsiteelementtypeid=42;sourcesiteelementtypeid=39;sourcesitetypeid=1;sourcesiteid=3582;destsiteid=3581;destsitetypeid=1',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'timestamp',binInterval)







        # SITE TO SITE-SITE TO TRANSIT
        # createScreenDataFile('transitsite','destsitetypeid=3;sourcesitetypeid=1;sourcesiteid=18;destsiteid=2971',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'PEAK_uplinkbytebuffer',binInterval)
        # createScreenDataFile('transitsite','destsitetypeid=10989;sourcesitetypeid=1;sourcesiteid=18;destsiteid=3',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'PEAK_uplinkbytebuffer',binInterval)

        # QuickTrends
        # createScreenDataFile('timestamp;destsiteid','destsitetypeid=1;sourcesitetypeid=1;sourcesiteid=18',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'timestamp',binInterval)
        # createScreenDataFile('timestamp;sourcesiteid','destsitetypeid=1;sourcesitetypeid=1;sourcesiteid=18;destsiteid=27',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'timestamp',binInterval)
        # createScreenDataFile('timestamp;sourcesiteid','sourcesitetypeid=1;sourcesiteid=18',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'timestamp',binInterval)
        # Site to Site-Site to NF to QTs
        # createScreenDataFile('timestamp;nfnameid','nfnameid=5;destsitetypeid=1;sourcesitetypeid=1;sourcesiteid=18;destsiteid=27',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'timestamp',binInterval)
        # NF TO SITE to QTrends
        # createScreenDataFile('timestamp;sourcesiteid','nfnameid=5;sourcesitetypeid=1;sourcesiteid=18',rawData_f_nrmca_60min_3600_sitedatacube,columns_f_nrmca_60min_3600_sitedatacube,'timestamp',binInterval)
        # createScreenDataFile('timestamp;nfnameid','nfnameid=5;sourcesitetypeid=1;sourcesiteid=18',rawData_f_nrmca_60min_3600_sitedatacube,columns_f_nrmca_60min_3600_sitedatacube,'timestamp',binInterval)



        print("Execution Time --- %s seconds ---" % (time.time() - startExecutionTime))
