def addOne(receivedFunc):
    def myFunc():
        return receivedFunc() + 1
    return myFunc

@addOne
def passFunc(x=2):
    return x

# passFunc = addOne(passFunc)
# print passFunc()


print passFunc()
print passFunc()
print passFunc()



pass