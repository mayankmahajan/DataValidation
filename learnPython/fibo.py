def createFibo(num):
    a = [0,1]
    sum = 0
    for i in range(0,num -2):
        sum = a[-2] + a[-1]
        a.append(sum)
    return a
if __name__ == '__main__':
    user_input = int(raw_input('num : '))
    print createFibo(user_input)
