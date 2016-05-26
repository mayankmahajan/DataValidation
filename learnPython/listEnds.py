import random
def endList(list):
    return [list[0],list[-1]]
    # return [list[0],list.pop()]
if __name__ == '__main__':

    list = random.sample(range(100),9)
    print list
    print endList(list)
