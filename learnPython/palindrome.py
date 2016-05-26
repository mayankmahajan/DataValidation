if __name__ == '__main__':
    while True:
        str = raw_input("Enter to check for palindrome :  ")
        if str == '':
            break
# method 1
#         iterations = len(str)/2 if len(str) % 2 == 0 else len(str)/2+1
#         isPalindrome = True
#         for i in range(0,iterations):
#             if str[i] != str[len(str)-i-1]:
#                 # print 'string is not palindrome'
#                 isPalindrome = False
#                 break
# method 2
        revStr = str[::-1]
        isPalindrome = True if revStr == str else False

        print 'Palindrome' if isPalindrome else 'Not palindrome'