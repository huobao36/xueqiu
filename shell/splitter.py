import os,sys


def linecount_wc(filename):
	return int(os.popen('wc -l ' + filename).read().split()[0])


def split_file(outdir, filename):
	count = 0
	linecount = linecount_wc(filename)
	lines = []
	rawFile = open(filename, 'r')
	fileidx = 0
	output = open(outdir +"/input"+ str(fileidx) , 'w')
	while 1:
		line = rawFile.readline()
		if line:
			count = count + 1
			lines.append(line)
			if	count < linecount / 8:
				output.write(line)
			elif count == linecount / 8:
				output.write(line)
				fileidx = fileidx + 1
				count = 0
				output.close()
				output = open(outdir + "/input"+ str(fileidx) , 'w') 		
			continue
		else:
			break
	rawFile.close()

def main():
	if len(sys.argv) < 2:  
	    print u'please input file name\n' 
	else:
		outdir = os.path.dirname(sys.argv[0])
		split_file(outdir, sys.argv[1])


main()
