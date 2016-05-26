#!/usr/bin/python
from optparse import OptionParser
import commands
import re,time 
import os, sys

columns_f_nrmca_60min_3600_nfcube = ['timestamp','nfnameid','isipv6','bytebuffer','flowbuffer','costbuffer']
columns_f_nrmca_60min_3600_sitedatacube = ['sourcesiteid', 'sourcesitetypeid', 'sourcesiteelementid', 'nfnameid', 'isipv6', 'timestamp','downlinkbytebuffer','uplinkbytebuffer','uplinkflowbuffer','downlinkflowbuffer','uplinkcostbuffer','downlinkcostbuffer']
columns_f_nrmca_60min_3600_siteflowdatacube = ['sourcesiteid', 'sourcesitetypeid', 'sourcesiteelementid', 'nfnameid', 'destsiteid', 'destsitetypeid', 'destsiteelementtypeid', 'isipv6', 'vrfid', 'timestamp','downlinkbytebuffer','uplinkbytebuffer','uplinkflowbuffer','downlinkflowbuffer','uplinkcostbuffer','downlinkcostbuffer']

uplinkColsForCSV =      {   'AGGR' : ['AGGR_uplinkbitbuffer','AGGR_uplinkbytebuffer','AGGR_uplinkflowbuffer','uplinkcostbuffer'],
                            'PEAK' : ['PEAK_uplinkbitbuffer','PEAK_uplinkbytebuffer','PEAK_uplinkflowbuffer','uplinkcostbuffer']    }
downlinkColsForCSV =    {   'AGGR' : ['AGGR_downlinkbitbuffer','AGGR_downlinkbytebuffer','AGGR_downlinkflowbuffer','downlinkcostbuffer'],
                            'PEAK' : ['PEAK_downlinkbitbuffer','PEAK_downlinkbytebuffer','PEAK_downlinkflowbuffer','downlinkcostbuffer']    }
upDownlinkColsForCSV =  {   'AGGR' : ['AGGR_totalbitbuffer','AGGR_totalByteBuffer','AGGR_totalflowbuffer','totalcostbuffer','AGGR_uplinkbytebuffer','AGGR_downlinkbytebuffer'],
                            'PEAK' : ['PEAK_totalbitbuffer','PEAK_totalByteBuffer','PEAK_totalflowbuffer','totalcostbuffer','PEAK_uplinkbytebuffer','PEAK_downlinkbytebuffer']    }

FH=open('rec_f_nrmca_60min_3600_sitedatacube_1451192400')      #Data for Dec27 05:00
# FH=open('rec_SITE_NF')
rawData_f_nrmca_60min_3600_sitedatacube=FH.readlines()
FH.close()

# FH=open('rec_f_nrmca_60min_3600_siteflowdatacube_1451192400')      #Data for Dec27 05:00
FH=open('rec_f_nrmca_60min_3600_siteflowdatacube_1451192400_destsiteid_27')
rawData_f_nrmca_60min_3600_siteflowdatacube=FH.readlines()
FH.close()



FH=open('rec_f_nrmca_60min_3600_nfcube_1451192400')
rawData_f_nrmca_60min_3600_nfcube=FH.readlines()
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
    elif key == 'sourcesiteid' or key == 'destsiteid':
        return siteIdDict[str(data)]
    elif key == 'sourcesitetypeid' or key == 'destsitetypeid':
        return siteTypeIdDict[str(data)]
    elif key == 'timestamp' or key == 'sourcesiteelementid' or key == 'destsiteelementtypeid' or key == 'isipv6' :
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

            if 'uplinkbytebuffer' in processedDataPerRecord.keys():
                processedDataPerRecord['PEAK_uplinkbitbuffer'] = (MaxFunc('uplinkbytebuffer', eval(processedDataPerRecord['uplinkbytebuffer'])) * 8) /300
                processedDataPerRecord['AGGR_uplinkbitbuffer'] = (Sum('uplinkbytebuffer', eval(processedDataPerRecord['uplinkbytebuffer'])) * 8) /3600
                processedDataPerRecord['PEAK_downlinkbitbuffer'] = (MaxFunc('downlinkbytebuffer', eval(processedDataPerRecord['downlinkbytebuffer'])) * 8) /300
                processedDataPerRecord['AGGR_downlinkbitbuffer'] = (Sum('downlinkbytebuffer', eval(processedDataPerRecord['downlinkbytebuffer'])) * 8) /3600
                processedDataPerRecord['PEAK_totalbitbuffer'] = processedDataPerRecord['PEAK_uplinkbitbuffer'] + processedDataPerRecord['PEAK_downlinkbitbuffer']
                processedDataPerRecord['AGGR_totalbitbuffer'] = processedDataPerRecord['AGGR_uplinkbitbuffer'] + processedDataPerRecord['AGGR_downlinkbitbuffer']


            for eachKey in processedDataPerRecord.keys():
                try:
                    processedDataPerRecord[eachKey] = eval(processedDataPerRecord[eachKey])
                except:
                    pass
                if type(processedDataPerRecord[eachKey]) is list and len(processedDataPerRecord[eachKey])>1:
                    processedDataPerRecord['AGGR_'+eachKey] = Sum('AGGR'+eachKey,processedDataPerRecord[eachKey])
                    processedDataPerRecord['PEAK_'+eachKey] = MaxFunc('Max'+eachKey,processedDataPerRecord[eachKey])

            processedDataPerRecord['AGGR_totalByteBuffer'] = processedDataPerRecord['AGGR_uplinkbytebuffer'] + processedDataPerRecord['AGGR_downlinkbytebuffer']
            processedDataPerRecord['PEAK_totalByteBuffer'] = processedDataPerRecord['PEAK_uplinkbytebuffer'] + processedDataPerRecord['PEAK_downlinkbytebuffer']
            processedDataPerRecord['AGGR_totalflowbuffer'] = processedDataPerRecord['AGGR_uplinkflowbuffer'] + processedDataPerRecord['AGGR_downlinkflowbuffer']
            processedDataPerRecord['PEAK_totalflowbuffer'] = processedDataPerRecord['PEAK_uplinkflowbuffer'] + processedDataPerRecord['PEAK_downlinkflowbuffer']
            processedDataPerRecord['totalcostbuffer'] = processedDataPerRecord['uplinkcostbuffer'][0] + processedDataPerRecord['downlinkcostbuffer'][0]

            processedData.append(processedDataPerRecord)
    return processedData

def whatToWrite(processedData,dimName,prevdimSelected,columnsForCSV,agg_peak):
    FH = openFileToWrite('outputs1/' + dimName + '_' +columnsForCSV[0] + '.csv' )
    cols = []
    cols.append(dimName)
    cols+=columnsForCSV
    result =''
    totalDict = {}
    newProcessedData = []
    newProcessedDataPerRecord = {}

    for eachRecord in processedData:

        arrayOfFilters = re.split(';',prevdimSelected)
        counter = 0
        for el in arrayOfFilters:
            if eachRecord[[re.split('=',el)][0][0]] != int([re.split('=',el)][0][1]):
                counter +=1
        if counter > 0 :
            continue

        if eachRecord[dimName] == newProcessedDataPerRecord[dimName] :
            newProcessedDataPerRecord[dimName] = eachRecord[dimName]
            
        for key in cols:
            if key != dimName :
                if key in totalDict:
                    totalDict[key] += GetNameFromId(key,eachRecord[key])
                else:
                    totalDict[key] = GetNameFromId(key,eachRecord[key])
    total = ''
    for eachRecord in processedData:

        arrayOfFilters = re.split(';',prevdimSelected)
        counter = 0
        for el in arrayOfFilters:
            if eachRecord[[re.split('=',el)][0][0]] != int([re.split('=',el)][0][1]):
                counter +=1
        if counter > 0 :
            continue

        # if eachRecord[[re.split('=',prevdimSelected)][0][0]] != int([re.split('=',prevdimSelected)][0][1]):
        #     continue



        for key in cols:
            if key != dimName:
                try:
                    result +=  str((GetNameFromId(key,eachRecord[key])/totalDict[key])*100) + ","
                except:
                    print
                total+= '100,'
            else:
                result +=  str(GetNameFromId(key,eachRecord[key])) + ","
                total = 'Total,'
        for key in cols:
            if key != dimName:
                result +=  str(GetNameFromId(key,eachRecord[key])) + ","
                total+= str(totalDict[key]) + ','
        result+='\n'

    result = result.replace(',\n','\n')
    FH.write(result)
    FH.write(total[:-1])
    FH.close()


def writeToCSVFile(processedData,dimName,sortField,columns,prevdimSelected):
    processedData = sorted(processedData, key=lambda k: k[sortField],reverse=True)
    whatToWrite(processedData,dimName,prevdimSelected,uplinkColsForCSV['AGGR'],'AGGR')
    whatToWrite(processedData,dimName,prevdimSelected,uplinkColsForCSV['PEAK'],'PEAK')
    whatToWrite(processedData,dimName,prevdimSelected,downlinkColsForCSV['AGGR'],'AGGR')
    whatToWrite(processedData,dimName,prevdimSelected,downlinkColsForCSV['PEAK'],'PEAK')
    whatToWrite(processedData,dimName,prevdimSelected,upDownlinkColsForCSV['AGGR'],'AGGR')
    whatToWrite(processedData,dimName,prevdimSelected,upDownlinkColsForCSV['PEAK'],'PEAK')


def createScreenDataFile(dimName,prevdimSelected,rawData,columns,sortField,binInterval):
    processedData = getProcessedData(rawData,columns,binInterval)
    writeToCSVFile(processedData,dimName,sortField,columns,prevdimSelected)


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

        # createScreenDataFile('nfnameid','sourcesiteid=18',rawData_f_nrmca_60min_3600_sitedatacube,columnSIteToNFScreen,'PEAK_uplinkbitbuffer',binInterval)
        # createScreenDataFile('sourcesiteid','nfnameid=5;sourcesitetypeid=1',rawData_f_nrmca_60min_3600_sitedatacube,columnSIteToNFScreen,'PEAK_uplinkbitbuffer',binInterval)

        createScreenDataFile('destsiteid','destsitetypeid=1;sourcesitetypeid=1;sourcesiteid=18',rawData_f_nrmca_60min_3600_siteflowdatacube,columns_f_nrmca_60min_3600_siteflowdatacube,'PEAK_uplinkbytebuffer',binInterval)



        print 'DONE...'
        # print 'Execution Time: ' + executionTime
        print("--- %s seconds ---" % (time.time() - startExecutionTime))