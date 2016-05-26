import random
from operator import add
from operator import sub
if __name__ == '__main__':
    rndNum = str(random.randint(1000,10000))
    rndNum = str(9111)
    rndNumList = [int(el) for el in str(9111)]
    attempts =0
    print rndNum
    while True:
        usr_inpt = str(raw_input('Enter 4 digit number : '))
        usr_inptList = [int(el) for el in usr_inpt]
        attempts +=1
        if usr_inpt == rndNum:
            print 'exact match'
            break
        diff = []
        diff = map(sub,rndNumList,usr_inptList)
        totalCows = 0
        maxBulls = 0
        totalCows = diff.count(0)
        maxBulls = 4 - totalCows
        nonZeroIndices = [i for i in range(0,4) if diff[i] != 0]
        counter = 0

        for j in nonZeroIndices:
            for k in nonZeroIndices:
                if usr_inpt[j] != rndNum[k] and (len(nonZeroIndices)==1 or j!=k):
                    counter +=1
            if counter < len(nonZeroIndices) or (counter == 1 and len(nonZeroIndices) == 1):
                maxBulls-=1
                counter = 0

        print totalCows
        print maxBulls

    print 'Attempts', attempts