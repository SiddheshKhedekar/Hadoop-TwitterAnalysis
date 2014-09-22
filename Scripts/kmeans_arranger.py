import sys
f = open(sys.argv[1], 'r')
f = f.readlines()
output = open('kmeansR.txt','w')
for i in f:
    clus, centr, vals =  i.split()[0],i.split()[1], "".join(i.split()[2:]).strip().split(',')
    [output.write("%s\t%s\n"%(i, clus)) for i in vals if i]
