#!/usr/bin/python
from optparse import OptionParser
import commands
import re,time 
import os, sys
from operator import itemgetter


columnNFScreen = ['timestamp','nfnameid','isipv6','bytebuffer','flowbuffer','costbuffer']
columnSIteToNFScreen = ['sourcesiteid', 'sourcesitetypeid', 'sourcesiteelementid', 'nfnameid', 'isipv6', 'timestamp','Downlinkbyte','upLinkByteBuffer','uplinkflowbuffer','downlinkflowbuffer','uplinkcostbuffer','downlinkcostbuffer']

FH=open('rec_SITE_NF')
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
        return data


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

def writeToFile(processedData,filename,sortField):
    FH = openFileToWrite(filename)
    processedData = sorted(processedData, key=lambda k: k[sortField],reverse=True)
    for eachKey in columnSIteToNFScreen:
        FH.write(eachKey + '\t')
    for eachRecord in processedData:
        result = '\n ----- \n\n'
        for key in eachRecord.keys():
            if ((type(eachRecord[key]) is list)  and len(eachRecord[key]) >1) == False :
                result +=  key +" : "+ str(GetNameFromId(key,eachRecord[key])) + ", "
        FH.write(result)
    FH.close()

def getProcessedData(rawData,columns):
    processedDataPerRecord = {}
    processedData = []
    for record in rawData:
        if re.match('^[0-9]',record):
            record = record.strip('\n')
            recordArray =  re.split('\t',record)
            processedDataPerRecord = dict(zip(columns,recordArray))
            for eachKey in processedDataPerRecord.keys():
                processedDataPerRecord[eachKey] = eval(processedDataPerRecord[eachKey])
                if type(processedDataPerRecord[eachKey]) is list and len(processedDataPerRecord[eachKey])>1:
                    processedDataPerRecord['AGGR_'+eachKey] = Sum('AGGR'+eachKey,processedDataPerRecord[eachKey])
                    processedDataPerRecord['Max_'+eachKey] = MaxFunc('Max'+eachKey,processedDataPerRecord[eachKey])
            processedData.append(processedDataPerRecord)
    return processedData

def createScreenDataFile(filename,rawData,columnNFScreen,sortField,binInterval):
    processedData = getProcessedData(rawData,columnNFScreen)
    writeToFile(processedData,filename,sortField)


def NFScreen(rawData,binInterval):
    processedData = getProcessedData(rawData,columnNFScreen)
    filename = 'NFScreen'
    writeToFile(processedData,filename)

def SiteToNFScreen(rawData,binInterval):
    processedData = getProcessedData(rawData,columnSIteToNFScreen)
    filename = 'SiteToNFScreen'
    writeToFile(processedData,filename)


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

        # NFScreen(rawData_NF,binInterval)
        # SiteToNFScreen(rawData_SITE_NF,binInterval)
        createScreenDataFile('SiteToNFScreenDataFile3',rawData_SITE_NF,columnSIteToNFScreen,'Downlinkbyte',binInterval)
        # createScreenDataFile('x_NFScreenDataFile',rawData_NF,columnNFScreen,'costbuffer',binInterval)

        print 'DONE...'