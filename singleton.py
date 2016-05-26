def singleton(myClass):
    instances = {}
    def getInstance(*args,**kwargs):
        if myClass not in instances:
            instances[myClass] = myClass(*args,**kwargs)
            return instances[myClass]
    return getInstance

@singleton
def addOne(x = 0):
    return x+1

print addOne()
print addOne()
print addOne()
pass
