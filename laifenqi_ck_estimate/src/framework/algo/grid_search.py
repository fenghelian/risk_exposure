#!/usr/bin/env python
__all__ = ['find_parameters']

import os, sys, traceback, getpass, time, re
import math,random
from threading import Thread
from subprocess import *

if sys.version_info[0] < 3:
	from Queue import Queue
else:
	from queue import Queue

telnet_workers = []
ssh_workers = []
nr_local_worker = 8

class GridOption:
	def __init__(self, options):
		dirname = os.path.dirname(__file__)
		self.app_pathname = os.path.join(dirname, '../')
		self.fold = 5
		self.c_begin, self.c_end, self.c_step = -1,  6,  1
		self.g_begin, self.g_end, self.g_step =  0, -8, -1
		self.p_begin, self.p_end, self.p_step = -8,  -1,  1
		self.grid_with_c, self.grid_with_g, self.grid_with_p = True, False, False
		self.pass_through_string = ' '
		self.resume_pathname = None
		self.parse_options(options)

	def parse_options(self, options):
		if type(options) == str:
			options = options.split()
		i = 0
		pass_through_options = []
		
		while i < len(options):
			if options[i] == '-c':
				i = i + 1
				if options[i] == 'null':
					self.grid_with_c = False
				else:
					self.c_begin, self.c_end, self.c_step = map(float,options[i].split(','))
			elif options[i] == '-g':
				i = i + 1
				if options[i] == 'null':
					self.grid_with_g = False
				else:
					self.g_begin, self.g_end, self.g_step = map(float,options[i].split(','))
			elif options[i] == '-p' :
				i = i + 1
				if options[i] == 'null':
					self.grid_with_p = False	
				else:
					self.p_begin, self.p_end, self.p_step = map(float,options[i].split(','))
			elif options[i] == '-v':
				i = i + 1
				self.fold = options[i]
			elif options[i] == '-app':
				i = i + 1
				self.app_pathname = options[i]
			elif options[i] == '-gnuplot':
				i = i + 1
				if options[i] == 'null':
					self.gnuplot_pathname = None
				else:	
					self.gnuplot_pathname = options[i]
			elif options[i] == '-png':
				i = i + 1
				self.png_pathname = options[i]
			elif options[i] == '-resume':
				if i == (len(options)-1) or options[i+1].startswith('-'):
					self.resume_pathname = self.dataset_title + '.out'
				else:
					i = i + 1
					self.resume_pathname = options[i]
			else:
				pass_through_options.append(options[i])
			i = i + 1

		self.pass_through_string = ' '.join(pass_through_options)
	
		if self.resume_pathname and not os.path.exists(self.resume_pathname):
			raise IOError('file for resumption not found')
		if not self.grid_with_c and not self.grid_with_g and not self.grid_with_p:
			raise ValueError('-c , -g and -p should not be null simultaneously')
		# gridregression default did not support gnuplot
		self.gnuplot_pathname = None
		if self.gnuplot_pathname and not os.path.exists(self.gnuplot_pathname):
			sys.stderr.write('gnuplot executable not found\n')
			self.gnuplot_pathname = None



def calculate_jobs(options):
	
	def range_f(begin,end,step):
		# like range, but works on non-integer too
		seq = []
		while True:
			if step > 0 and begin > end: break
			if step < 0 and begin < end: break
			seq.append(begin)
			begin = begin + step
		return seq
	
	def permute_sequence(seq):
		n = len(seq)
		if n <= 1: return seq
	
		mid = int(n/2)
		left = permute_sequence(seq[:mid])
		right = permute_sequence(seq[mid+1:])
	
		ret = [seq[mid]]
		while left or right:
			if left: ret.append(left.pop(0))
			if right: ret.append(right.pop(0))
			
		return ret	

	
	c_seq = permute_sequence(range_f(options.c_begin,options.c_end,options.c_step))
	g_seq = permute_sequence(range_f(options.g_begin,options.g_end,options.g_step))
	p_seq = permute_sequence(range_f(options.p_begin,options.p_end,options.p_step))
	
	if not options.grid_with_c:
		c_seq = [None]
	if not options.grid_with_g:
		g_seq = [None] 
	if not options.grid_with_p:
		p_seq = [None]
	
	nr_c = len(c_seq)
	nr_g = len(g_seq)
	nr_p = len(p_seq)
	jobs = []

	for i in range(0,nr_c):
		for j in range(0,nr_g):
			for s in range(0,nr_p):
				line=[]
				line.append((c_seq[i],g_seq[j],p_seq[s]))
				jobs.append(line)
	resumed_jobs = {}
	
	if options.resume_pathname is None:
		return jobs, resumed_jobs

	for line in open(options.resume_pathname, 'r'):
		line = line.strip()
		rst = re.findall(r'mse=([0-9.]+)',line)
		if not rst: 
			continue
		mse = float(rst[0])

		c, g, p = None, None, None 
		rst = re.findall(r'c=([0-9.-]+)',line)
		if rst: 
			c = float(rst[0])
		rst = re.findall(r'g=([0-9.-]+)',line)
		if rst: 
			g = float(rst[0])
		rst = re.findall(r'p=([0-9,-]+)',line)
		if rst:
			p = float(rst[0])

		resumed_jobs[(c,g,p)] = mse

	return jobs, resumed_jobs

	
class WorkerStopToken:  # used to notify the worker to stop or if a worker is dead
	pass

class Worker(Thread):
	def __init__(self,name,job_queue,result_queue,options, mse_callback):
		Thread.__init__(self)
		self.name = name
		self.job_queue = job_queue
		self.result_queue = result_queue
		self.options = options
		self.callback = mse_callback 
	def run(self):
		while True:
			(cexp,gexp,pexp) = self.job_queue.get()
			if cexp is WorkerStopToken:
				self.job_queue.put((cexp,gexp,pexp))
				# print('worker {0} stop.'.format(self.name))
				break
			try:
				c, g, p = None, None, None
				if cexp != None:
					c = cexp
				if gexp != None:
					g = gexp
				if pexp != None:
					p = pexp
				mse = self.run_one(c,g,p)
				if mse is None: print('get no mse')
			except:
				# we failed, let others do that and we just quit
			
				traceback.print_exception(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
				
				self.job_queue.put((cexp,gexp,pexp))
				sys.stderr.write('worker {0} quit.\n'.format(self.name))
				break
			else:
				self.result_queue.put((self.name,cexp,gexp,pexp,mse))

	def get_cmd(self,c,g,p):
		options=self.options
		cmdline = ' '
		if options.grid_with_c: 
			cmdline += ' {0} '.format(c)
		if options.grid_with_g: 
			cmdline += ' {0} '.format(g)
		if options.grid_with_p:
			cmdline += ' {0}' .format(p)

		return cmdline
		
class LocalWorker(Worker):
	def run_one(self,c,g,p):
		cmdline = self.get_cmd(c,g,p)
		#result = Popen(cmdline,shell=True,stdout=PIPE,stderr=PIPE,stdin=PIPE).stdout
		
		mse = self.callback(c)
		return float(mse)

class SSHWorker(Worker):
	def __init__(self,name,job_queue,result_queue,host,options):
		Worker.__init__(self,name,job_queue,result_queue,options)
		self.host = host
		self.cwd = os.getcwd()
	def run_one(self,c,g,p):
		cmdline = 'ssh -x -t -t {0} "cd {1}; {2}"'.format\
			(self.host,self.cwd,self.get_cmd(c,g,p))
		result = Popen(cmdline,shell=True,stdout=PIPE,stderr=PIPE,stdin=PIPE).stdout
		for line in result.readlines():
			if str(line).find('Cross') != -1:
				return float(line.split()[-1])

class TelnetWorker(Worker):
	def __init__(self,name,job_queue,result_queue,host,username,password,options):
		Worker.__init__(self,name,job_queue,result_queue,options)
		self.host = host
		self.username = username
		self.password = password		
	def run(self):
		import telnetlib
		self.tn = tn = telnetlib.Telnet(self.host)
		tn.read_until('login: ')
		tn.write(self.username + '\n')
		tn.read_until('Password: ')
		tn.write(self.password + '\n')

		# XXX: how to know whether login is successful?
		tn.read_until(self.username)
		# 
		print('login ok', self.host)
		tn.write('cd '+os.getcwd()+'\n')
		Worker.run(self)
		tn.write('exit\n')			   
	def run_one(self,c,g,p):
		cmdline = self.get_cmd(c,g,p)
		result = self.tn.write(cmdline+'\n')
		(idx,matchm,output) = self.tn.expect(['Cross.*\n'])
		for line in output.split('\n'):
			if str(line).find('Cross') != -1:
				return float(line.split()[-1])
			
def find_parameters(options, mse_callback):
	
	def update_param(c,g,p,mse,best_c,best_g,best_p,best_mse,worker,resumed):
		if ( mse < best_mse):
			best_mse,best_c,best_g,best_p = mse,c,g,p
		stdout_str = '[{0}] {1} {2} (best '.format\
			(worker,' '.join(str(x) for x in [c,g,p] if x is not None),mse)
		output_str = ''
		if c != None:
			stdout_str += 'c={0}, '.format(best_c)
			output_str += 'c={0} '.format(c)
		if g != None:
			stdout_str += 'g={0}, '.format(best_g)
			output_str += 'g={0} '.format(g)
		if p != None:
			stdout_str += 'p={0}, '.format(best_p)
			output_str += 'p={0} '.format(p)
		stdout_str += 'mse={0})'.format(best_mse)
		print(stdout_str)
		return best_c,best_g,best_p,best_mse
		
	options = GridOption(options);
	
	# put jobs in queue

	jobs,resumed_jobs = calculate_jobs(options)
	job_queue = Queue(0)
	result_queue = Queue(0)

	for (c,g,p) in resumed_jobs:
		result_queue.put(('resumed',c,g,p,resumed_jobs[(c,g,p)]))

	for line in jobs:
		for (c,g,p) in line:
			if (c,g,p) not in resumed_jobs:
				job_queue.put((c,g,p))

	# hack the queue to become a stack --
	# this is important when some thread
	# failed and re-put a job. It we still
	# use FIFO, the job will be put
	# into the end of the queue, and the graph
	# will only be updated in the end
 
	job_queue._put = job_queue.queue.appendleft

	# fire telnet workers

	if telnet_workers:
		nr_telnet_worker = len(telnet_workers)
		username = getpass.getuser()
		password = getpass.getpass()
		for host in telnet_workers:
			worker = TelnetWorker(host,job_queue,result_queue,
					 host,username,password,options)
			worker.start()

	# fire ssh workers

	if ssh_workers:
		for host in ssh_workers:
			worker = SSHWorker(host,job_queue,result_queue,host,options)
			worker.start()

	# fire local workers

	for i in range(nr_local_worker):
		worker = LocalWorker('local thread-' + str(i),job_queue,result_queue,options, mse_callback)
		worker.start()

	# gather results

	done_jobs = {}

	db = []
	best_mse = float('+inf')
	best_c,best_g,best_p = None,None,None  

	for (c,g,p) in resumed_jobs:
		mse = resumed_jobs[(c,g,p)]
		best_c,best_g,best_p,best_mse = update_param(c,g,p,mse,best_c,best_g,best_p,best_mse,'resumed',True)

	for line in jobs:
		for (c,g,p) in line:
			while (c,g,p) not in done_jobs:
				(worker,c1,g1,p1,mse1) = result_queue.get()
				done_jobs[(c1,g1,p1)] = mse1
				if (c1,g1,p1) not in resumed_jobs:
					best_c,best_g,best_p,best_mse = update_param(c1,g1,p1,mse1,best_c,best_g,best_p,best_mse,worker,False)
			db.append((c,g,p,done_jobs[(c,g,p)]))


	job_queue.put((WorkerStopToken,None,None))
	best_param, best_cgp  = {}, []
	if best_c != None:
		best_param['c'] = best_c
		best_cgp += [best_c]
	if best_g != None:
		best_param['g'] = best_g
		best_cgp += [best_g]
	if best_p != None:
		best_param['p'] = best_p
		best_cgp += [best_p]
	print('{0} {1}'.format(' '.join(map(str,best_cgp)), best_mse))

	return best_mse, best_param


def mse_callback(params):
	time.sleep(4)
	return random.randint(1000, 100000)

if __name__ == '__main__':
	
	options = "-c 0.75,0.83,0.01"
	try:
		t1 = time.time()
		find_parameters(options, mse_callback)
		print time.time() - t1
	except (IOError,ValueError) as e:
		sys.stderr.write(str(e) + '\n')
		sys.stderr.write('Try "grid_search.py" for more information.\n')
		sys.exit(1)
	
