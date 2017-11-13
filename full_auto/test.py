import re
timefilter = re.compile(r'\d+/\d+/\d+\s\d+:\d+:\d+\s|\d+-\d+-\d+\s\d+:\d+:\d+\,\d+\s')
numberfilter = re.compile(r'(\d+|\d+.\d+)(\s|,|\)|\(|]|.)')
URIfilter = re.compile(r'(http://.*?\s|spark://.*?\s)')
IPfilter = re.compile(r'\d+\.\d+\.\d+\.\d+(\s|:\d+)')
Levelfilter = re.compile(r'INFO\s|WARN\s')
bracketsfilter = re.compile(r'\(.*?\)|\)')
filefilter = re.compile(r'')
classfilter =re.compile(r'^(.+?)- .+')
spacefilter = re.compile(r'  ')
row = "2017-08-16 19:30:30,284 INFO  spark.SecurityManager (Logging.scala:logInfo(54)) - Changing view acls to: hadoop"
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
print 40 * ''
cla = classfilter.match(row).group(1)
print row
print cla