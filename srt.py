#!/usr/bin/env python

import sys
import codecs
import optparse
DEFAULT_CODECS="GB18030,UTF-8,UTF-16,UTF-7"
DEFAULT_GUESSES=["gb18030", "utf-8", "utf-16", "utf-7"]

def format_time(t):
	ms=t % 1000
	s=t/1000%60
	m=t/60000%60
	h=t/3600000%60
	return "%02d:%02d:%02d,%03d" % (h, m, s, ms)

def stotime(s):
	(h, m, sms)=s.split(":")
	(s, ms)=sms.split(",")
	return int(ms)+int(s)*1000+int(m)*60000+int(h)*3600000

def line_str(lines):
	s=""
	for l in lines:
		if len(l)>0:
			if len(s)>0:
				s=s+"\n"
			s=s+l
	if len(s)==0:
		return "--"
	return s

class timerange(object):
	def __init__(self, start=0, end=1):
		if end>=start:
			self.start=start
			self.end=end
		else:
			self.start=end
			self.end=start
	def __len__(self):
		return self.end-self.start
	def __eq__(self, other):
		return (self.start == other.start) and (self.end == other.end)
	def __ne__(self, other):
		return not(self==other)
	def __mul__(self, scale):
		self.start=int(scale*self.start)
		self.end=int(scale*self.end)
		return self
	def __add__(self, offset):
		self.start=self.start+offset
		self.end=self.end+offset
		return self
	def __str__(self):
		return "%s --> %s" % (format_time(self.start), format_time(self.end))
	def read(self, s, offset=0, scale=1):
		i=s.find("-->")
		st=s[0:i].strip()
		et=s[i+3:].strip()
		self.start=stotime(st)
		self.end=stotime(et)
		self=(self+offset)*scale
	def clone(self):
		return timerange(self.start, self.end)
	def inrange(self, t):
		return (t>=self.start) and (t<self.end)
	def intersected(self, other):
		return self.inrange(other.start) or self.inrange(other.end-1) or other.inrange(self.start) or other.inrange(self.end-1)

class srtitem(object):
	def __init__(self, tr=None, lines=[], lines2=[]):
		if isinstance(tr, basestring):
			self.tr=timerange()
			self.tr.read(tr)
		elif isinstance(tr, timerange):
			self.tr=tr.clone()
		else:
			self.tr=timerange()
		self.lines=[]
		self.lines.extend(lines)
		self.lines.extend(lines2)
	def __mul__(self, scale):
		self.tr.start=int(scale*self.tr.start)
		self.tr.end=int(scale*self.tr.end)
	def __add__(self, offset):
		self.tr.start=self.tr.start+offset
		self.tr.end=self.tr.end+offset
	# Comparisons, only use to sort srtitems
	def __lt__(self, other):
		return self.tr.start < other.tr.start
	def __gt__(self, other):
		return self.tr.start > other.tr.start
	def __le__(self, other):
		return self.tr.start <= other.tr.start
	def __ge__(self, other):
		return self.tr.start >= other.tr.start
	def __eq__(self, other):
		return self.tr.start == other.tr.start
	def __ne__(self, other):
		return self.tr.start <> other.tr.start
	def __str__(self):
		return "%s\n%s" % (str(self.tr), line_str(self.lines))
	def clone(self):
		return srtitem(self.start, self.length, self.lines)
	def addlines(self, line):
		if isinstance(line, basestring):
			self.lines.append(line)
		else:
			for l in line:
				self.lines.append(l)
	def intersect(self, other):
		if not(self.tr.intersected(other.tr)):
			return None
		if self.tr.start<other.tr.start:
			# Self starts first
			tr1=srtitem(timerange(self.tr.start, other.tr.start), self.lines)
			if self.tr.end<other.tr.end:
				# Self ends first
				tr2=srtitem(timerange(other.tr.start, self.tr.end), self.lines, other.lines)
				tr3=srtitem(timerange(self.tr.end, other.tr.end), other.lines)
				return(tr1, tr2, tr3)
			elif self.tr.end==other.tr.end:
				# Both end at same time
				tr2=srtitem(timerange(other.tr.start, other.tr.end), self.lines, other.lines)
				return (tr1, tr2)
			else:
				# Other ends first
				tr2=srtitem(timerange(other.tr.start, other.tr.end), self.lines, other.lines)
				tr3=srtitem(timerange(other.tr.end, self.tr.end), self.lines)
				return(tr1, tr2, tr3)
		elif self.tr.start==other.tr.start:
			# Both start at same time
			if self.tr.end<other.tr.start:
				# Self ends first, first has both lines, second has other lines
				tr1=srtitem(timerange(self.tr.start, self.tr.end), self.lines, other.lines)
				tr2=srtitem(timerange(self.tr.end, other.tr.end), other.lines)
				return (tr1, tr2)
			elif self.tr.end==other.tr.end:
				# Same time range, merge lines, returns a single item list
				return (srtitem(timerange(self.tr.start, self.tr.end), self.lines, other.lines), )
			else:
				# Other ends first, first has both lines, second has self lines
				tr1=srtitem(timerange(self.tr.start, other.tr.end), self.lines, other.lines)
				tr2=srtitem(timerange(other.tr.end, self.tr.end), self.lines)
				return (tr1, tr2)
		else:
			# Other starts first
			tr1=srtitem(timerange(other.tr.start, self.tr.start), other.lines)
			if self.tr.end<other.tr.end:
				# Self ends first
				tr2=srtitem(timerange(self.tr.start, self.tr.end), self.lines, other.lines)
				tr3=srtitem(timerange(self.tr.end, other.tr.end), other.lines)
				return(tr1, tr2, tr3)
			elif self.tr.end==other.tr.end:
				# Both end at same time
				tr2=srtitem(timerange(self.tr.start, other.tr.end), self.lines, other.lines)
				return (tr1, tr2)
			else:
				# Other ends first
				tr2=srtitem(timerange(self.tr.start, other.tr.end), self.lines, other.lines)
				tr3=srtitem(timerange(other.tr.end, self.tr.end), self.lines)
				return(tr1, tr2, tr3)

class srt(object):
	def __init__(self, src=None):
		self.items=[]
		if src:
			self.load(src)
	def __len__(self):
		return len(self.items)
	def __getitem__(self, i):
		return self.items[i]
	def load(self, s, offset=0, scale=1, threshold=1, strip_pattern=""):
		# (Time+offset)*scale
		# Read and parse SRT file
		ln=0
		state=1
		n=0
		tr=timerange()
		lines=[]
		try:
			for l in s:
				ln=ln+1
				l=l.strip()
				if state==1:
					# State 1, reading No., which will be ignored as there are a lot of inconsistency in SRT files out there
					if len(l)<=0:
						# Handle malformed srt, which may contain extra empty lines between items
						continue
					n=int(l)
					state=2
				elif state==2:
					# State 2, reading time, which is the actual base of order
					if len(l)<=0:
						continue
					# Apply offset and scale
					tr.read(l, offset, scale)
					state=3
				elif state==3:
					# State 3, reading text
					if(len(l))>0:
						# There still are texts, keep in state 3
						# TODO: apply strip pattern
						lines.append(l)
					else:
						# Text ends, new srtitem, then reset lines
						ss=srtitem(tr, lines)
						self.add(ss, threshold)
						lines=[]
						state=1
				else:
					# Error state!
					pass
		except:
			print "Error in line ", ln
			raise
	def find_index(self, s):
		# Find index the srt item should be inserted into
		for i in range(len(self.items)):
			if self.items[i].tr.intersected(s.tr):
				return i
			if self.items[i]>=s:
				return i
		return len(self.items)
	def add(self, s, threshold=1):
		if len(s.tr)<=threshold:
			return
		# Keep list sorted, and every append cause a serie of intersection check, which can cause more insertion
		i=self.find_index(s)
		if i+1>len(self.items):
			# Append to the last
			self.items.append(s)
			return
		srts=self.items[i].intersect(s)
		if srts is None:
			# Not intersected with next one, just insert it
			self.items.insert(i, s)
		else:
			# Intersected with next one, remove next one and re-insert all intersections
			self.items.pop(i)
			for srt in srts:
				self.add(srt)
	def write(self, o):
		# Dump content into a SRT file
		n=1
		for s in self.items:
			o.write(unicode(n)+"\n"+unicode(s)+"\n\n")
			#print >> o, n
			#print >> o, s
			#print >> o
			n=n+1

def guess_codec(fn, guesses=DEFAULT_GUESSES):
	for g in guesses:
		try:
			f=codecs.open(fn, "r", g)
			list(f)
			f=codecs.open(fn, "r", g)
			return f
		except UnicodeError:
			# Try next
			continue
	print >> sys.stderr, "Cannot decode file ", fn, " with any codecs."
	return None

def mergesrt(outsrt, fn, codec_guesses, offset, scale, threshold):
	f=guess_codec(fn, codec_guesses)
	if f is None:
		print >> sys.stderr, "Skipping file ", fn
		return
	outsrt.load(f, offset, scale, threshold)

def loadsrt(outsrt, option, opt, value, parser):
	#print parser.values.scale*100, parser.values.offset, value
	codecs=parser.values.codecs.split(',')
	mergesrt(outsrt, value, codecs, parser.values.offset, parser.values.scale, parser.values.threshold)

def savesrt(outsrt, fn, codec):
	if fn=="-":
		outsrt.write(sys.stdout)
		return
	outsrt.write(codecs.open(fn, "w", codec))

if __name__=="__main__":
	outsrt=srt()
	loadsrt_c=lambda op, o, v, p: loadsrt(outsrt, op, o, v, p)
	usage = "usage: %prog [options]" 
	parser=optparse.OptionParser(usage=usage)
	parser.add_option("-o", "--output", dest="ofn", default="-", help="output srt FILE", metavar="FILE")
	parser.add_option("-i", "--input", action="callback", callback=loadsrt_c, type="string", help="input srt FILE", metavar="FILE")
	parser.add_option("-t", "--offset", dest="offset", type="int", default=0, help="time offset of next input file, in milliseconds", metavar="OFFSET")
	parser.add_option("-s", "--scale", dest="scale", type="float", default=1, help="time scale of next input file, float number", metavar="SCALE")
	parser.add_option("-c", "--codecs", dest="codecs", default=DEFAULT_CODECS, help="CODECS used to read next input file", metavar="CODEC[,CODEC...]")
	parser.add_option("-C", "--output-codec", dest="ocodec", default="GB18030", help="CODEC used to write output file", metavar="CODEC")
	parser.add_option("-r", "--thresold", dest="threshold", type="int", default=1, help="subtitles last for time shorter than threshold will be dropped", metavar="THRESHOLD")
	(options, args)=parser.parse_args()
	if len(outsrt)==0:
		parser.print_help()
	else:
		savesrt(outsrt, parser.values.ofn, parser.values.ocodec)
