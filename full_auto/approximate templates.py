#coding=utf-8
from __future__ import division
from sklearn.cluster import KMeans
from collections import defaultdict, OrderedDict,Counter
from matplotlib import pyplot as plot
import Levenshtein as lev
import numpy as np
import re
timefilter = re.compile(r'\d+/\d+/\d+\s\d+:\d+:\d+\s|\d+-\d+-\d+\s\d+:\d+:\d+\,\d+\s')
numberfilter = re.compile(r'(\d+|\d+.\d+)(\s|,|\)|\(|]|.)')
URIfilter = re.compile(r'(http://.*?\s|spark://.*?\s)')
IPfilter = re.compile(r'\d+\.\d+\.\d+\.\d+(\s|:\d+)')
Levelfilter = re.compile(r'INFO\s|WARN\s')
bracketsfilter = re.compile(r'\(.*?\)|\)')
classfilter =re.compile(r'^(.+?)- .+')
spacefilter = re.compile(r'  ')

class node:
	def __init__(self, s, c):
		self.c = c
		self.s = s
		self.p = self
		self.rank = 0

def union(x, y):
	link(findset(x), findset(y))

def link(x, y):
	if x.rank > y.rank:
		y.p = x
	else:
		x.p = y
		if x.rank == y.rank:
			y.rank += 1

def findset(x):
	if x != x.p:
		x.p = findset(x.p)
	return x.p





l =  defaultdict(set)
p = set()
with open('../test/full_auto', 'r') as f, open('Erasing2.txt', 'w') as w:
	for row in f.readlines(): 
		# print row 
		if row == "17/08/13 15:38:00 INFO Registering block manager 192.168.100.9:47839 with 53.2 GB RAM, BlockManagerId(driver, 192.168.100.9, 47839)":
			row = bracketsfilter.sub('', row)
			print row
			row = timefilter.sub('',row)
			print row
			row = IPfilter.sub('', row)
			print row
			row = spacefilter.sub(' ', row)
			print row
			row = numberfilter.sub('', row) 
			print row
			row = URIfilter.sub('', row)
			print row
			row = Levelfilter.sub('', row)
			print row
		row = bracketsfilter.sub('', row)
		row = timefilter.sub('',row)
		row = IPfilter.sub('', row)
		row = spacefilter.sub(' ', row)
		row = numberfilter.sub('', row) 
		row = URIfilter.sub('', row)
		row = Levelfilter.sub('', row)
		if(classfilter.match(row) == None):
			print row
		cla = classfilter.match(row).group(1)
		if row[-1] != '\n':
			row = row + '\n'
		# print cla
		# print row
		l[cla].add(row)
		w.write(row)
		p.add(row)


p = list(p)
print p
## 进一步划分log key
# q = defaultdict(str)
# r = list()
# for idx, st in enumerate(p):
# 	q[idx] = st
# 	temp = node(idx, st)
# 	r.append(temp)

# distance = OrderedDict()

# for i in range(len(p)):
# 	for j in range(i + 1, len(p)):
# 		if lev.distance(q[i], q[j]) < 29:
# 			union(r[i], r[j])

# l = defaultdict(list)
# for i in range(len(p)):
# 	l[findset(r[i]).c].append(r[i].c)

# for i in l:
# 	for j in l[i]:
# 		print j
# 	print 50 * '-'







						




## 使用kmean寻找阈值
# count= Counter(e)
# plot.figure(figsize=(15, 10))
# plot.scatter(count.keys(), count.values())
# plot.show()
# kmeans = KMeans(n_clusters=2, max_iter=100, n_init=10).fit(d)
# print kmeans.cluster_centers_
# Max = 0
# for i in range(len(kmeans.labels_)):
# 	if kmeans.labels_[i] == 1 and Max < d[i][0]:
# 		Max = d[i][0]
# print Max
		

