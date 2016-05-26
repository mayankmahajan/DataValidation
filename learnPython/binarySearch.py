if __name__ == '__main__':
    num = 91
    list = [1,3,5,7,9,10,13,17,23,26]
    length = len(list)
    sIndex = 0
    lIndex = len(list) -1
    if num in list:
        print True
    while True:

        mIndex = (-sIndex +lIndex)/2


        if list[mIndex] == num:
            print 'found'
            break
        if mIndex<sIndex or mIndex>lIndex or mIndex<0:
            print 'not found'
            break
        elif list[mIndex] > num:
            lIndex = mIndex
        else:
            sIndex = mIndex


