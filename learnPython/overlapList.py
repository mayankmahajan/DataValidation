import random
if __name__ == '__main__':

# method 1
    # a = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    # b = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    # c =[]
    # for el in a:
    #     if el in b and el not in c:
    #         c.append(el)
    # print c


# method 2
#     a = random.sample(range(100),20)
#     b = random.sample(range(100),30)

    a = [12, 43, 2, 30, 23, 22, 50, 92, 57, 33, 48, 78, 44, 96, 63, 70, 60, 9, 51, 69]
    b = [62, 18, 47, 45, 66, 93, 27, 91, 17, 86, 58, 9, 70, 78, 68, 1, 50, 61, 30, 80, 32, 81, 83, 24, 89, 33, 98, 3, 31, 76]
    print a
    print b
    a = set(a)
    b = set(b)
    print list(set(a) & set(b))

