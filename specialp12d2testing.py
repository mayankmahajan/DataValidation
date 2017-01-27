from HalfPrecisionFloat import *
from operator import add
from copy import deepcopy

# from numpy import sum


def getArrays(hexValue):
    tempArr = []
    for i in range(12):
        try:
            tempArr.append(shortBitsToFloat(int(hexValue[i*4:i*4+4],16)))
        except:
            print hexValue[i*4:i*4+4]
    return tempArr

def getTuple(hexValue):
    tempArr = []
    for i in range(12):
        try:
            tempArr.append(shortBitsToFloat(int(hexValue[i*4:i*4+4],16)))
        except:
            print hexValue[i*4:i*4+4]
    # return tempArr
    sum = reduce(add,tempArr)
    peak = max(tempArr)
    return (sum,peak)


def convertbufferToTuple(record):
    r = {}
    for k,v in record.iteritems():
        if 'buffer' in k and 'transit' not in k:
            r[k] = getTuple(v)
        else:
            if 'transit' in k:
                r[k] = v
            else:

                r[k] = int(v)
    return r


def getMaxPerTime(t,position):
    return max([reduce(add,[(el[position][i]) for el in t]) for i in range(len(t[0][position]))])


if __name__ == '__main__':
    position = 13
    timstampPosition=12
    # fh = open('/Users/mayank.mahajan/sitedata101')
    # fh = open('/Users/mayank.mahajan/sdatacube')
    # fh = open('/Users/mayank.mahajan/interface2')
    fh = open('/Users/mayank.mahajan/sfdatacube1')
    # fh = open('/Users/mayank.mahajan/sitedatas')
    # fh = open('/Users/mayank.mahajan/nfdatas')





    rawData = fh.readlines()
    # print rawData
    a = []
    for el in rawData:
        x=el.strip().split("[")[1].split("]")[0].split(',')
        if x[1] == '3582' and  x[2] == '1' and  x[5] == '148':
            a.append(el.strip().split("[")[1].split("]")[0].split(','))
        # a.append(el.strip().split("[")[1].split("]")[0].split(','))

    # print a
    b = deepcopy(a)

    for e in b:
        e[position]=getTuple(e[position][16:])

    # print b
    s = 0
    for ele in b:
        s+=ele[position][0]
    # print s

    c = deepcopy(a)
    for e in c:
        e[position]=getArrays(e[position][16:])

    s1 =[0] * 12
    l1 = 12
    for i in range(0,len(c)):
        for j in range(l1):
            s1[j] = s1[j]+c[i][position][j]

    print reduce(add,s1)


    t1 =[]
    t2 = []
    for el in c:
        if el[timstampPosition] == '1469325600':
            t1.append(el)
        elif el[timstampPosition] == '1469322000':
            t2.append(el)

    print getMaxPerTime(t1,position)
    print getMaxPerTime(t2,position)

    #
    # print max([max(el[13]) for el in t1])
    # print max([max(el[13]) for el in t2])
    # #
    # #
    # sum1 =[0] * 12
    # l1 = 12
    # for i in range(0,len(c)):
    #     for j in range(l1):
    #         sum1[j] = sum1[j]+c[i][position][j]
    #     # s1 = sum([s1,c[i]], axis=0)
    # getMaxperTime(t1,)
    #     max([reduce(add,[(el[13][i]) for el in t1]) for i in range(12)])
    # # three = sum([c], axis=0)
    # print max(sum1)
    # print reduce(add,sum1)
    # print sum1
    # #
    # # sum2 =[0] * 12
    # # l1 = 12
    # # for i in range(1,len(c)):
    # #     for j in range(l1):
    # #         sum2[j] = sum2[j]+c[i][position][j]
    # #     # s1 = sum([s1,c[i]], axis=0)
    # #
    # # # three = sum([c], axis=0)
    # # print max(sum2)
    # # print reduce(add,sum2)
    # # print sum2
    #
