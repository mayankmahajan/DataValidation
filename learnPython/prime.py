def getDivisors(num):
    a = []
    for i in range(1,(num/2)+1):
        if num % i ==0:
            a.append(i)
    a.append(num)
    return a
def checkPrime(num,divisors):
    print str(num) + ' is prime' if len(divisors) <= 2 else str(num) + ' is not prime'

if __name__ == '__main__':
    num = 0
    while True:
        a = []
        num = int(raw_input("enter a number : "))
        if num ==0:
            break
        a = getDivisors(num)
        checkPrime(num,a)
