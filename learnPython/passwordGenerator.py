import random

def generatePassword(strength):
    lowerCap = False
    upperCap = False
    numChar = False
    specialChar = True
    password = ''

    while True:
        randomInt = random.randrange(32,126)
        if randomInt in range(48,58):
            numChar = True
            password = password + chr(randomInt)
        elif randomInt in range(65,91):
            upperCap = True
            password = password + chr(randomInt)
        elif randomInt in range(97,123):
            lowerCap = True
            password = password + chr(randomInt)
        else:
            specialChar = True
            password = password + chr(randomInt)
        if strength == 's' and numChar and upperCap and lowerCap and specialChar and len(password)>=6:
            return password
        if strength == 'w' and len(password)>=6:
            return password


if __name__ == '__main__':
    strength = raw_input("Enter strength w/s: ")
    print generatePassword(strength)




