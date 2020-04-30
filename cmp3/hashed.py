import sys
import numpy as np

class HashedSet(object):
	def __init__(self, f):
		self.F = f
		self.Hash

class HashTable(object):

	def __init__(self, file_reader):
		#
		#    Hashes: set of all hash values for words found in the list
		#    Dups: a dictionary: { hash_value -> set( all the words with this hash value }
		#	x_dups dictionary contains *only* hashes, for which there are duplicates
		#	in most cases x_dups will be empty or small
		#
		x_hashes = set()
		x_dups = {}
		file_reader.rewind()
		for w in file_reader.lines():
			h = hash(w)
			if h in x_hashes:
				dup_paths = x_dups.get(h)
				if dup_paths is None:
					x_dups[h] = set([w])
				else:
					dup_paths.add(w)
			else:
				x_hashes.add(h)
		# do we need to rescan ?
		if len(x_dups):
			file_reader.rewind()
			for w in file_reader.lines():
				h = hash(w)
				if h in x_dups: x_dups[h].add(w)
		self.Hashes = x_hashes
		self.Dups = x_dups

	def __contains__(self, w):
		h = hash(w)
		if not h in self.Hashes:	return False
		if not h in self.Dups:		return True


def diff(x_words, y_hashes, y_dups):
	#
	# calculates X-Y
	#
	x_minus_y = set()
	for w in x_words:
		h = hash(w)
		if h in y_hashes:
			if h in y_dups:
				
	

	
				
	

f = open("/tmp/a.list", "r")
hashes = set()
n_dups = 0
while True:
	l = f.readline()
	if not l:	break
	h = hash(l)
	if h in hashes:
		n_dups += 1
	else:
		hashes.add(h)

print (n_dups)
