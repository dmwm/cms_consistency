import sys

n = sum(int(n) for _, n in (line.strip().split() for line in sys.stdin))
print(n)
	
