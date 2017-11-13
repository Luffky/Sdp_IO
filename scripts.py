#coding=utf-8
import numpy as np
import re
from collections import defaultdict, OrderedDict
def matchLog(filename):
	'''
		正则匹配日志

		参数：filename，日志文件名，类型：list
		返回：log，格式化后的日志条目，每条是一个dict，有四个键值对，分别为data日期，level等级，class源，content内容，类型list[dict{}]
	'''
	logpat = re.compile(r'^(\d+-\d+-\d+\s\d+:\d+:\d+\,\d+)\s(INFO|WARN|ERROR)\s\s.*?\.(.*)\s.+-\s(.*)$')
	log = []
	for file in filename:
		print str(file) + '...'
		with open(file, 'r') as f:
			for row in f.readlines():
				temp = logpat.match(row)
				if temp == None:
					continue
				t = {}

				t['date'] = temp.group(1)
				t['level'] = temp.group(2)
				t['class'] = temp.group(3)
				t['class'] = t['class']
				t['content'] = temp.group(4)			
				log.append(t)
	return log


def classifyLog(log):
	'''
		按日志来源对日志进行分类

		参数：log，日志条目，类型：list[dict{}]
		返回：d，分好类之后的日志条目，每个来源对应一个list，类型defaultdict，默认value为list类型吧
	'''
	d = defaultdict(list)
	for row in log:
		d[row['class']].append(row['date'] + ' ' + row['level'] + ' ' + row['content'])
	return d


def writeClassfiedLog(d, filename):
	'''
		将分好类的日志存入文件

		参数：d，使用classifyLog分好类的日志条目，类型defaultdict，默认value为list类型
		参数：filename，保存的文件名，类型string
	'''
	with open(filename, 'w') as f:
		for num, Class in enumerate(d):
			f.write(str(num) + ': ' + Class + ' total: ' + str(len(d[Class])) + '\n')
			for row in d[Class]:
				f.write(row + '\n')
			f.write('--------------------------------------------------\n')

def writeClassfiedLogInDifferentFile(d):
	'''
		将分好类的日志按类名存入文件

		参数：d，使用classifyLog分好类的日志条目，类型defaultdict，默认value为list类型
	'''
	for num, Class in enumerate(d):
		with open(Class, 'w') as f:
			f.write(str(num) + ': ' + Class + ' total: ' + str(len(d[Class])) + '\n')
			for row in d[Class]:
				f.write(row + '\n')
			f.write('--------------------------------------------------\n')


def readClassfiedLog(filename):
	'''
		将分好类的日志读入内存

		参数：filename，读取文件名，类型string
		返回：d，分好类之后的日志条目，每个来源对应一个list，类型defaultdict，默认value为list类型
	'''
	classpat = re.compile(r'^(\d+:)\s(.*?)\stotal:\s(\d+)\n$')
	d = defaultdict(list)
 	with open(filename, 'r') as f:
 		text = f.readlines()
 		num = len(text)
 		i = 0
 		while(i < num):
	 		m = classpat.match(text[i])
	 		i += 1
	 		if m != None:
	 			up = i + int(m.group(3))
	 			while(i < up):
					logpat = re.compile(r'^(\d+-\d+-\d+\s\d+:\d+:\d+\,\d+)\s(INFO|WARN|ERROR)\s(.*)$')
					temp = logpat.match(text[i])
					d[m.group(2)].append(temp.group(1) + ' ' + temp.group(2) + ' '+ temp.group(3))
					i += 1
	return d

def blockManagerInfo(logOfBlockManagerInfo):
	'''
		解析BlockManagerInfo类产生的日志，记录broadcast占据内存量
		参数：logOfBlockManagerInfo，BlockManagerInfor类产生的日志，类型list
		返回：d，记录了added和removed的总内存量,单位为MB，类型defaultdict，value为float
	'''
	d = defaultdict(float)
	logpat = re.compile(r'^\d+-\d+-\d+\s\d+:\d+:\d+\,\d+\s(INFO|WARN|ERROR)\sAdded\sbroadcast_(\d+)_piece0.*size:\s(.*?)(B|KB|MB|GB),\sfree:\s(.*?)(B|KB|MB|GB)')
	for row in logOfBlockManagerInfo:
		m = logpat.match(row)
		if m != None:
			if m.group(4) == 'B':
				d[m.group(2)] += round(float(m.group(3)) / 1024 / 1024, 5)
			elif m.group(4) == 'KB':
				d[m.group(2)] += round(float(m.group(3)) / 1024, 5)
			elif m.group(4) == 'MB':
				d[m.group(2)] += round(float(m.group(3)), 5)
			elif m.group(4) == 'GB':
				d[m.group(2)] += (float(m.group(3)) * 1024, 5)
			else:
				assert 1 == 0
	return d

def DAGScheduler(logOfDAGScheduler):
	'''
		解析DAGScheduler类产生的日志
		参数：DAGScheduler，DAGscheduler类产生的日志，类型list
		返回：d，每个stage的task数量
	'''

	ret = []
	d = OrderedDict()
	logpat = re.compile(r'^\d+-\d+-\d+\s\d+:\d+:\d+\,\d+\s(INFO|WARN|ERROR)\sSubmitting\s(\d+).+(ShuffleMap|Result)Stage\s(\d+).+$')
	for log in logOfDAGScheduler:
		m = logpat.match(log)
		if m != None:
			if m.group(3) == 'ShuffleMap':
				d['ShuffleMapStage' + m.group(4)] = int(m.group(2))
			else:
				d['ResultStage' + m.group(4)] = int(m.group(2))
	return d




# def mapOutputTrackerMasterEndpoint(logOfMapOutputTrackerMasterEndpoint):
# 	'''
# 		解析MapOutputTrackerMasterEndpoint类产生的日志
# 	'''
# 	pass

def taskSetManager(logOfTaskSetManager):
	'''
		解析TaskSetManager类产生的日志
		参数：TaskSetManager，TaskSetManager类产生的日志，类型list
		返回：ret，关于task的各种信息，类型(dict, dict, dict)，每个dict有两个键值对分别为num和memory
	'''
	
	recalculatedTask = {'num': 0, 'memory(MB)': 0.0}
	PROCESS_LOCAL = {'num': 0, 'memory(MB)': 0.0}
	NODE_LOCAL = {'num': 0, 'memory(MB)': 0.0}
	logpat = re.compile(r'^\d+-\d+-\d+\s\d+:\d+:\d+\,\d+\s(INFO|WARN|ERROR)\sStarting\stask\s\d+\.(\d+).+(PROCESS_LOCAL|NODE_LOCAL),\s(\d+)\sbytes\)$')
	for log in logOfTaskSetManager:
		m = logpat.match(log)
		if m != None:
			if m.group(2) == '0':
				if m.group(3) == 'PROCESS_LOCAL':
					PROCESS_LOCAL['num'] += 1
					PROCESS_LOCAL['memory(MB)'] += float(m.group(4)) / 1024 / 1024
				elif m.group(3) == 'NODE_LOCAL':
					NODE_LOCAL['num'] += 1
					NODE_LOCAL['memory(MB)'] += float(m.group(4)) / 1024 / 1024
			else:
				recalculatedTask['num'] += 1
				recalculatedTask['memory(MB)'] += float(m.group(4)) / 1024 / 1024
	PROCESS_LOCAL['memory(MB)'] = round(PROCESS_LOCAL['memory(MB)'], 3)
	NODE_LOCAL['memory(MB)'] = round(NODE_LOCAL['memory(MB)'], 3)
	recalculatedTask['memory(MB)'] = round(recalculatedTask['memory(MB)'], 3)
	return PROCESS_LOCAL, NODE_LOCAL, recalculatedTask


def taskSetManager2(logOfTaskSetManager):
	'''
		解析TaskSetManager类产生的日志
		参数：TaskSetManager，TaskSetManager类产生的日志，类型list
		返回：ret，每个stage的内存占用defaultdict(float), int
	'''
	memory = defaultdict(float)
	logpat = re.compile(r'^\d+-\d+-\d+\s\d+:\d+:\d+\,\d+\s(INFO|WARN|ERROR).+in stage\s(\d+).+(PROCESS_LOCAL|NODE_LOCAL),\s(\d+)\sbytes\)$')
	for log in logOfTaskSetManager:
		m = logpat.match(log)
		if m != None:
			memory[m.group(2)] += float(m.group(4)) / 1024 / 1024
	return memory
			

def memoryStore(logOfMemoryStore):
	'''
		解析MemoryStore类产生的日志
		参数：logOfMemoryStore，LogOfMemoryStore类产生的日志，类型list
		返回：ret，memory占有量，单位MB，类型default(float)
	'''
	ret = defaultdict(float)
	logpat = re.compile(r'^\d+-\d+-\d+\s\d+:\d+:\d+\,\d+\s(INFO|WARN|ERROR)\sBlock broadcast_\d+(\sstored|_piece\d+).+?\(estimated\ssize\s(.+?)(B|KB|MB|GB).*$')
	for log in logOfMemoryStore:
		m = logpat.match(log)
		if m != None:
			if m.group(4) =='B':
				ret[m.group(2)[1:]] += round(float(m.group(3)) / 1024 / 1024, 3)
			elif m.group(4) == 'KB':
				ret[m.group(2)[1:]] += round(float(m.group(3)) / 1024, 3)
			elif m.group(4) == 'MB':
				ret[m.group(2)[1:]] += round(float(m.group(3)), 3)
			elif m.group(4) == 'GB':
				ret[m.group(2)[1:]] += round(float(m.group(3)) * 1024, 3)
	return ret


def mapOutputTrackerMaster(logOfMapOutputTrackerMaster):
	'''
		解析MapOutputTrackerMaster类产生的日志
		参数：logOfMapOutputTrackerMaster，MapOutputTrackerMaster类产生的日志，类型list
		返回：ret，总的shuffle量，单位MB，类型float

	'''
	logpat = re.compile(r'^\d+-\d+-\d+\s\d+:\d+:\d+\,\d+\s(INFO|WARN|ERROR)\sSize\sof\soutput\sstatuses\sfor\sshuffle\s\d+\sis\s(\d+)\sbytes$')
	ret = 0 
	for log in logOfMapOutputTrackerMaster:
		m = logpat.match(log)
		if m != None:
			ret += float(m.group(2))
	return round(ret / 1024 / 1024, 3)
			
def analyzeLog(d, nameOfClass):
	'''
		解析不同类产生的日志
		参数：d，使用classifyLog分好类的日志条目，类型defaultdict，默认value为list类型
		参数：nameOfClass，解析的类名，类型String
		返回：解析后的结果，根据解析的类名不同而类型不定
	'''
	if nameOfClass=='BlockManagerInfo':
		return blockManagerInfo(d[nameOfClass])

	elif nameOfClass=='MapOutputTrackerMaster':
		return mapOutputTrackerMaster(d[nameOfClass])

	elif nameOfClass=='MemoryStore':
		return memoryStore(d[nameOfClass])

	elif nameOfClass=='TaskSetManager':
		return taskSetManager(d[nameOfClass])

	elif nameOfClass=='TaskSetManager2':
		return taskSetManager2(d['TaskSetManager'])

	elif nameOfClass=='DAGScheduler':
		return DAGScheduler(d['DAGScheduler'])

def makeClassfiedLog(filepath=['log.log']):
	log = matchLog(filepath)
	d = classifyLog(log)
	writeClassfiedLog(d, 'divide.txt')

makeClassfiedLog()



d = readClassfiedLog('divide.txt')

with open('statistic.txt', 'w') as fi:
	e = analyzeLog(d, 'MapOutputTrackerMaster')
	fi.write( 'shuffleStatuses: ' + str(e) + 'MB\n')
	fi.write( 40 * '-' + '\n')

	f = analyzeLog(d, 'MemoryStore')
	fi.write( 'braodcast')
	for row in f:
		if row == 'stored':
			fi.write( 'values' + ':' + str(f[row]) + 'MB\n')
		elif row =='piece0':
			fi.write( 'bytes' + ': ' + str(f[row]) + 'MB\n')
	fi.write( 40 * '-' + '\n')

	c = analyzeLog(d, 'BlockManagerInfo')
	totalTasks = 0
	totalMemory = 0.0
	jobTasks = 0
	jobMemory = 0.0
	g = analyzeLog(d, 'DAGScheduler')
	job = 0
	fi.write('job: ' + str(job) + '\n')
	for ind, row in enumerate(g):
		fi.write( row + " task: " + str(g[row]) + ', memory used: ' + str(c[str(ind)]) + 'MB ' + '\n')
		stagepat = re.compile(r'^ResultStage\d+$')
		jobTasks += g[row]
		jobMemory += c[str(ind)]
		if stagepat.match(row) != None:
			job += 1
			fi.write('jobTasks: ' + str(jobTasks) + '\n')
			fi.write('jobMemory: ' + str(jobMemory) + '\n')
			totalTasks += jobTasks
			totalMemory += jobMemory
			if ind < len(g) - 1:
				fi.write('\n'+ 'job: ' + str(job) + '\n')
				jobTasks = 0
				jobMemory = 0


	fi.write('\n')
	fi.write( 'totalTasks: ' + str(totalTasks) + '\n')
	fi.write( 'totalMemory: ' + str(totalMemory) + ' MB\n')
	fi.write( 40 * '-' + '\n')

	a = analyzeLog(d, 'TaskSetManager2')
	for row in a:
		fi.write( 'stage' + row + ' ' + str(round(a[row], 3)) + 'MB\n')
	b1, b2, b3 = analyzeLog(d, 'TaskSetManager')
	
	for row in b1:
		fi.write( 'PROCESS_LOCAL: ' + row + ':' +  str(b1[row]) + '\n')
	for row in b2:
		fi.write( 'NODE_LOCAL: ' + row + ':' + str(b2[row]) + '\n')
	for row in b3:
		fi.write( 'Recalculated: ' + row + ':' + str(b3[row]) + '\n')
	fi.write( 40 * '-' + '\n')


