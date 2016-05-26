#!/usr/bin/python
from optparse import OptionParser
import commands
import re,time 
import os, sys

columnNFScreen = ['timestamp','nfnameid','isipv6','bytebuffer','flowbuffer','costbuffer']
columnSIteToNFScreen = ['sourcesiteid', 'sourcesitetypeid', 'sourcesiteelementid', 'nfnameid', 'isipv6', 'timestamp','downlinkByteBuffer','uplinkByteBuffer','uplinkflowbuffer','downlinkflowbuffer','uplinkcostbuffer','downlinkcostbuffer']

uplinkColsForCSV = {    'AGGR' : ['AGGR_uplinkBitBuffer','AGGR_uplinkByteBuffer','AGGR_uplinkflowbuffer','uplinkcostbuffer'],
                        'PEAK' : ['PEAK_uplinkBitBuffer','PEAK_uplinkByteBuffer','PEAK_uplinkflowbuffer','uplinkcostbuffer']    }

downlinkColsForCSV = {  'AGGR' : ['AGGR_downlinkBitBuffer','AGGR_downlinkByteBuffer','AGGR_downlinkflowbuffer','downlinkcostbuffer'],
                        'PEAK' : ['PEAK_downlinkBitBuffer','PEAK_downlinkByteBuffer','PEAK_downlinkflowbuffer','downlinkcostbuffer']    }

FH=open('rec_SITE_NF_1451192400')      #Data for Dec27 05:00
# FH=open('rec_SITE_NF')
rawData_SITE_NF=FH.readlines()
FH.close()

FH=open('rec_NF')
rawData_NF=FH.readlines()
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


def MaxFunc(key,MaxList):
    try:
        # MaxList=eval(MaxList)
        return max(MaxList)
    except:
        return GetNameFromId(key,MaxList)

def GetNameFromId(key,data):
    if key == 'nfnameid':
        return nfIdDict[str(data)]
    elif key == 'sourcesiteid':
        return siteIdDict[str(data)]
    elif key == 'sourcesitetypeid':
        return siteTypeIdDict[str(data)]
    elif key == 'timestamp' or key == 'sourcesiteelementid' or key == 'isipv6' :
        return data
    else:
        return data[0] if (type(data) is list and len(data) == 1) else data

def Sum(key,ComponentList):
    sumElements=0
    try:
        # ComponentList=eval(ComponentList)
        for element in ComponentList:
            sumElements+=float(element)
        return sumElements
    except:
        return GetNameFromId(key,ComponentList)

def openFileToWrite(filename):
    if os.path.isfile(filename):
        os.system('rm -f '+ filename)
    # else:
    #     if os.path.isfile(filename):
    #         return open(filename,'a')
    #     else:
    return open(filename,'w')

def getProcessedData(rawData,columns,binInterval):
    processedDataPerRecord = {}
    processedData = []
    for record in rawData:
        if re.match('^[0-9]',record):
            record = record.strip('\n')
            recordArray =  re.split('\t',record)
            processedDataPerRecord = dict(zip(columns,recordArray))

            if 'uplinkByteBuffer' in processedDataPerRecord.keys():
                processedDataPerRecord['PEAK_uplinkBitBuffer'] = (MaxFunc('uplinkByteBuffer', eval(processedDataPerRecord['uplinkByteBuffer'])) * 8) /300
                processedDataPerRecord['AGGR_uplinkBitBuffer'] = (Sum('uplinkByteBuffer', eval(processedDataPerRecord['uplinkByteBuffer'])) * 8) /3600

            for eachKey in processedDataPerRecord.keys():
                try:
                    processedDataPerRecord[eachKey] = eval(processedDataPerRecord[eachKey])
                except:
                    pass
                if type(processedDataPerRecord[eachKey]) is list and len(processedDataPerRecord[eachKey])>1:
                    processedDataPerRecord['AGGR_'+eachKey] = Sum('AGGR'+eachKey,processedDataPerRecord[eachKey])
                    processedDataPerRecord['PEAK_'+eachKey] = MaxFunc('Max'+eachKey,processedDataPerRecord[eachKey])
            processedData.append(processedDataPerRecord)
    return processedData

def whatToWrite(processedData,dimName,prevdimSelected,columnsForCSV,agg_peak):
    FH = openFileToWrite(dimName + agg_peak + '.csv' )
    cols = []
    cols.append(dimName)
    cols+=columnsForCSV
    result =''
    totalDict = {}

    for eachRecord in processedData:
        if eachRecord[[re.split('=',prevdimSelected)][0][0]] != int([re.split('=',prevdimSelected)][0][1]):
            continue
        for key in cols:
            if key != dimName :
                if key in totalDict:
                    totalDict[key] += GetNameFromId(key,eachRecord[key])
                else:
                    totalDict[key] = GetNameFromId(key,eachRecord[key])
    total = ''
    for eachRecord in processedData:
        if eachRecord[[re.split('=',prevdimSelected)][0][0]] != int([re.split('=',prevdimSelected)][0][1]):
            continue
        for key in cols:
            if key != dimName:
                result +=  str((GetNameFromId(key,eachRecord[key])/totalDict[key])*100) + ","
                total+= '100,'
            else:
                result +=  str(GetNameFromId(key,eachRecord[key])) + ","
                total = 'Total,'
        for key in cols:
            if key != dimName:
                result +=  str(GetNameFromId(key,eachRecord[key])) + ","
                total+= str(totalDict[key]) + ','
        result+='\n'
    FH.write(result)
    FH.write(total)
    FH.close()


def writeToCSVFile(processedData,dimName,sortField,columns,prevdimSelected):
    processedData = sorted(processedData, key=lambda k: k[sortField],reverse=True)
    whatToWrite(processedData,dimName,prevdimSelected,uplinkColsForCSV['AGGR'],'AGGR')
    whatToWrite(processedData,dimName,prevdimSelected,uplinkColsForCSV['PEAK'],'PEAK')

def createScreenDataFile(dimName,prevdimSelected,rawData,columns,sortField,binInterval):
    processedData = getProcessedData(rawData,columns,binInterval)
    writeToCSVFile(processedData,dimName,sortField,columns,prevdimSelected)


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

        createScreenDataFile('nfnameid','sourcesiteid=18',rawData_SITE_NF,columnSIteToNFScreen,'PEAK_uplinkBitBuffer',binInterval)

        print 'DONE...'