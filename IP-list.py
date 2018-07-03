# coding: utf-8

from resources import Settings

def main():
    out = ''
    count = 0
    for host in Settings.hosts:
        if count % 19 == 0 and count != 0:
            print(out)
            out = ''
        out += host[0] + ';' 
        count += 1
    print(out)


if __name__ == '__main__':
    main()
