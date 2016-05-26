def reverse(usr_in):
    return ' '.join(usr_in.split()[::-1])

if __name__ == '__main__':
    usr_in = str(raw_input("enter sentence : "))
    print reverse(usr_in)