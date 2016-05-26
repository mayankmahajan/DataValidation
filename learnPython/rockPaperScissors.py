def playGame(p1,p2):
    w = ['r','s','p']
    l = ['s','p','r']
    if p1 and p2 not in w:
        print 'Please enter valid string'
        return
    if p1 == p2 :
        print 'tie'
    else:
        if p2 == l[w.index(p1)]:
            print p1 + ' : p1 is winner'
        else:
            print p2 + ' : p2 is winner'
if __name__ == '__main__':
    while True:
        user_input1 = raw_input("enter1 : ")
        if user_input1 == '':
            break
        user_input2 = raw_input("enter2 : ")
        playGame(user_input1,user_input2)


