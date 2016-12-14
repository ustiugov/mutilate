#!/usr/bin/env python

import os
import select
import signal
import subprocess
import sys
import threading

class Runner:
  def __init__(self):
    self.procs = {}
    self.unblockr, self.unblockw = os.pipe()
    t = threading.Thread(target = self.reader)
    t.daemon = True
    t.start()

  def execute(self, id, cmdline):
    proc = subprocess.Popen(cmdline, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE, preexec_fn = os.setsid)
    self.procs[id] = proc
    os.write(self.unblockw, 'x')

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    for id, proc in self.procs.items():
      os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
      proc.wait()

  def reader(self):
    buf = {}

    while True:
      for id in self.procs:
        if id + '/out' not in buf:
          buf[id + '/out'] = ''
          buf[id + '/err'] = ''
      rlist = [self.unblockr]
      rlist += [proc.stdout for proc in self.procs.itervalues()]
      rlist += [proc.stderr for proc in self.procs.itervalues()]
      rlist, _, _ = select.select(rlist, [], [])
      if self.unblockr in rlist:
        os.read(self.unblockr, 1)
      delete = []
      for id, proc in self.procs.iteritems():
        if proc.stdout in rlist:
          data = os.read(proc.stdout.fileno(), 4096)
          if len(data) == 0:
            delete.append(id)
          buf[id + '/out'] += data
        if proc.stderr in rlist:
          buf[id + '/err'] += os.read(proc.stderr.fileno(), 4096)
      for d in delete:
        status = self.procs[d].wait()
        print >>sys.stderr, '%s: exit status %s' % (d, status)
        del self.procs[d]
      for tag in buf:
        lines = buf[tag].split('\n')
        if not buf[tag].endswith('\n'):
          buf[tag] = lines[-1]
        else:
          buf[tag] = ''
        del lines[-1]
        for line in lines:
          print >>sys.stderr, '%s: %s' % (tag, line)

def getopt(args, short, long):
  for i in xrange(len(args)):
    if args[i].startswith('%s=' % long):
      return args[i].split('=')[1]
    elif i+1 < len(args) and (args[i] == long or args[i] == short):
      return args[i+1]
  return None

def rmopt(args, short, long):
  for i in xrange(len(args)):
    if args[i].startswith('%s=' % long):
      del args[i]
      break
    elif i+1 < len(args) and (args[i] == long or args[i] == short):
      del args[i:i+2]
      break
  return args

def main():
  agents = getopt(sys.argv, '-a', '--agent')
  master_agent = getopt(sys.argv, None, '--master-agent')
  sys.argv = rmopt(sys.argv, None, '--master-agent')

  assert master_agent is not None

  if agents is None:
    agents = []
  else:
    agents = agents.split(',')

  cwd = os.path.dirname(os.path.realpath(__file__))
  for agent in [master_agent] + agents:
    subprocess.call(['ssh', agent, 'pkill', 'mutilate'])
    subprocess.check_call('scp -q %s/mutilate %s:/tmp' % (cwd, agent), shell = True)

  with Runner() as runner:
    for agent in agents:
      runner.execute(agent, 'ssh %s stdbuf -oL /tmp/mutilate --agentmode --threads=16 < /dev/null' % agent)
    try:
      subprocess.check_call(['ssh', master_agent, '/tmp/mutilate'] + sys.argv[1:], stdin=open('/dev/null', 'r'))
    except:
      subprocess.check_call(['ssh', master_agent, 'pkill', 'mutilate'])
      pass

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print
