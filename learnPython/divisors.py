if __name__ == '__main__':
    num = 0
    while True:
        num = int(raw_input("enter a number : "))
        if num ==0:
            break
        for i in range(1,(num/2)+1):
            if num % i ==0:
                print i
        print num