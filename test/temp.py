
import re
line = []
with open('full_auto', 'r') as f:
	for row in f.readlines():
		logpat = re.compile(r'^(\d+-\d+-\d+\s\d+:\d+:\d+\,\d+)\s(INFO|WARN|ERROR)\s\s(.*)(\s\(.+\)\))\s-\s(.*)$')
		m = logpat.match(row)
		if m != None:
			line.append(m.group(1) + ' ' + m.group(3) + ' ' + m.group(5))
		else:
			line.append(row)
with open('simplied.log', 'w') as f:
	for row in line:
		f.write(row + '\n')