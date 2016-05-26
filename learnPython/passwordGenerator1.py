import random
import string


if __name__ == '__main__':
    chars=string.ascii_letters + string.digits + string.punctuation
    print chars
    print random.sample(chars,6)
    print ''.join(random.choice(chars) for _ in range(6))

    print random.randrange(3)



