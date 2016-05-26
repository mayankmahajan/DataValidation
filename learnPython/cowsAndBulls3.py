
import random
import sys

def cowcheck(X, guess_X,):
    if X == guess_X:
        return 1
    else:
        return 0


def bullcheck(zahl, guess_zahl):
    a = list(zahl)
    b = guess_zahl

    i = 0
    while i < len(a):
            if a[i] == b[i]:
                del a[i]
                del b[i]
                i = 0
            else:
                i += 1
    bullscount = 0
    i = 0
    while i < len(a):
        j = 0
        while j < len(b):
            if a[i] == b[j]:
                bullscount += 1
                del a[i]
                del b[j]
                j = 0
                i = 0
            else:
                j += 1
        i += 1
    return bullscount


if __name__ == "__main__":
    again = "yes"
    print("Welcome to Cows and Bulls! A Cow is a correct number at the correct spot,\n"
          "a Bull is a correct number but in the wrong place.\n")
    while again == "yes":

        start = "yes"
        mistake = "no"
        count = 0
        cows = 0
        A = random.randint(0, 9)
        B = random.randint(0, 9)
        C = random.randint(0, 9)
        D = random.randint(0, 9)
        zahl = (A, B, C, D)
        zahl = (9,1,1,1)
        print zahl

        while cows < 4:
            try:
                if mistake == "yes":
                    guess = raw_input("Please only type a 4-digit number or 'exit'. ")
                    mistake = "no"
                elif start == "no":
                    guess = raw_input("Try again: ")
                else:
                    guess = raw_input("Guess the 4-digit number: ")
                    start = "no"
                if guess == "exit" or guess == "EXIT":
                    guess = "exit"
                    print("The correct number was " + str(zahl))
                    again = raw_input("Would you like to play again? ")
                    while guess == "exit":
                        if again == "yes":
                            break
                        elif again == "no" or again == "exit" or again == "EXIT":
                            sys.exit()
                        else:
                            again = raw_input("Please type 'yes' or 'no'. ")

                if guess == "exit":
                    break

                elif len(guess) != 4:
                    mistake = "yes"
                    continue

                guess_A = int(guess[0])
                guess_B = int(guess[1])
                guess_C = int(guess[2])
                guess_D = int(guess[3])
                guess_zahl = [guess_A, guess_B, guess_C, guess_D]
                print guess_zahl

                cows = cowcheck(A, guess_A) + cowcheck(B, guess_B) + cowcheck(C, guess_C) + cowcheck(D, guess_D)
                bulls = bullcheck(zahl, guess_zahl)
                count += 1

                print("cows = " + str(cows) + ", bulls = " + str(bulls))

            except ValueError:
                mistake = "yes"

            if cows == 4:
                print("You got it right! You only needed " + str(count) + " tries.\n")
                again = raw_input("Would you like to play again? ")
                while again != "no" and again != "yes" and again != "exit" and again != "EXIT":
                    again = raw_input("Please answer with 'yes' or 'no'. ")
                if again == "no" or again == "exit" or again == "EXIT":
                    sys.exit()