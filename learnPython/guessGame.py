import random

if __name__ == '__main__':
    num = random.randint(1,9)
    print 'Number : ' + str(num)
    user_input = 0
    attempts = 0
    while True:
        user_input = int(raw_input('enter number'))
        if user_input == 'exit':
            print 'Total attempts : ' + str(attempts)
            break
        attempts = attempts + 1
        diff = user_input - num
        if (diff ==0):
            print 'You guessed it right...'
            print 'Total attempts : ' + str(attempts)
            break
        elif (diff) > 0:
            print 'entered bigger number'
        else:
            print 'entered smaller number'
