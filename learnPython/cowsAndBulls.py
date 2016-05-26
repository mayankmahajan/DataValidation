import random
from operator import add
if __name__ == '__main__':
    cows = []
    bulls = []
    rndNum = str(random.randint(1000,10000))
    rndNum = str(9119)
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
                cows.append(1)
                same = True
            else:
                cows.append(0)
                same = False
            if usr_inpt[i] in rndNum and same != True:
                bulls.append(1)
            else:
                bulls.append(0)

        # for i in range(0,4):
        #     if bulls[i]>0:
        #         usr_inpt[i].in
        print cows, reduce(lambda x,y : x+y,cows)
        print bulls, reduce(lambda x,y : x+y,bulls)
        cows=[]
        bulls=[]
    print 'Attempts', attempts