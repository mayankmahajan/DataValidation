from __future__ import with_statement

def writeToFile(text,path,mode):
    with open(path,mode) as open_file:
        open_file.write(text)

if __name__ == '__main__':
    text = 'Hello!!!'
    mode = 'a'
    path = 'hello.txt'
    writeToFile(text,path,mode)
