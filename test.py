import time
startExecutionTime = time.time();
sum =0
for ele in range(1,11):
  sum +=ele
print sum
print("Execution Time --- %s seconds ---" % (time.time() - startExecutionTime))


startExecutionTime1 = time.time();
n=10
print (n*(n+1)/2)

print("Execution Time --- %s seconds ---" % (time.time() - startExecutionTime1))