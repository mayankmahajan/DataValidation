import random
from operator import add
if __name__ == '__main__':
    cows = []
    bulls = []
    rndNum = str(random.randint(1000,10000))
    rndNum = str(9111)
    attempts =0
    print rndNum
    while True:
        usr_inpt = str(raw_input('Enter 4 digit number : '))
        attempts +=1
        if usr_inpt == rndNum:
            print 'exact match'
            break
        for i in range(0,4):
            same = False
            if rndNum[i] == usr_inpt[i]:
                cows.append(str(rndNum[i]))
                same = True
            else:
                cows.append('-1')
                same = False
            if usr_inpt[i] in rndNum and same != True:
                bulls.append(str(usr_inpt[i]))
            else:
                bulls.append('-1')
        totalCows = []
        totalCows = [1 if int(el) > -1 else 0 for el in cows ]
        totalCows = reduce(lambda x,y:x+y ,totalCows)
        totalBulls = []
        totalBulls = [1 if int(el) > -1 else 0 for el in bulls ]
        totalBulls = reduce(lambda x,y:x+y ,totalBulls)
        print totalCows,totalBulls
        print cows
        print bulls
        # count = 0
        # for i in range(0,4):
        #     if bulls[i] != '-1':
        #         for j in range(0,4):
        #
        #             if cows[j] == '-1':
        #                 if bulls[i] == rndNum[j]:
        #                     count +=1
        #             totalBulls = totalBulls -






        cows=[]
        bulls=[]
    print 'Attempts', attempts