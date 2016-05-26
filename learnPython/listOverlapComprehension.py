# write a program that returns a list that contains only the elements that are common between the lists (without duplicates).
# Make sure your program works on two lists of different sizes.

import random
if __name__ == '__main__':
    a = random.sample(range(100),20)
    b = random.sample(range(100),30)
    print a
    print b
    uniqueCommon = []
    uniqueCommon = [el for el in a if (el in b and el not in uniqueCommon)]
    print uniqueCommon
