import sys
f =open(sys.argv[1],'r')
f = f.readlines()
output = open('shortest2R.txt','w')

for i in f:
    j,k,l = i.split()
    for x in l.split(':'):
        if x:
            output.write("%s;%s\n"%(j,x))
 
