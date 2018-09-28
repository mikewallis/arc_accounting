# Python library providing useful Gridengine functions

import os
import re

# DEBUG:
# - each regex definition should just be in the scope of, and near to,
#   the function using it.
# - need to embed function documentation in a pythonic way, rather
#   than as comments

record_def = re.compile(r"""
   (?P<qname>[^:#]+)
   :(?P<hostname>[^:]+)
   :(?P<group>[^:]+)
   :(?P<owner>[^:]+)
   :(?P<job_name>[^:]+)
   :(?P<job_number>[^:]+)
   :(?P<account>[^:]+)
   :(?P<priority>[^:]+)
   :(?P<submission_time>[^:]+)
   :(?P<start_time>[^:]+)
   :(?P<end_time>[^:]+)
   :(?P<failed>[^:]+)
   :(?P<exit_status>[^:]+)
   :(?P<ru_wallclock>[^:]+)
   :(?P<ru_utime>[^:]+)
   :(?P<ru_stime>[^:]+)
   :(?P<ru_maxrss>[^:]+)
   :(?P<ru_ixrss>[^:]+)
   :(?P<ru_ismrss>[^:]+)
   :(?P<ru_idrss>[^:]+)
   :(?P<ru_isrss>[^:]+)
   :(?P<ru_minflt>[^:]+)
   :(?P<ru_majflt>[^:]+)
   :(?P<ru_nswap>[^:]+)
   :(?P<ru_inblock>[^:]+)
   :(?P<ru_oublock>[^:]+)
   :(?P<ru_msgsnd>[^:]+)
   :(?P<ru_msgrcv>[^:]+)
   :(?P<ru_nsignals>[^:]+)
   :(?P<ru_nvcsw>[^:]+)
   :(?P<ru_nivcsw>[^:]+)
   :(?P<project>[^:]+)
   :(?P<department>[^:]+)
   :(?P<granted_pe>[^:]+)
   :(?P<slots>[^:]+)
   :(?P<task_number>[^:]+)
   :(?P<cpu>[^:]+)
   :(?P<mem>[^:]+)
   :(?P<io>[^:]+)
   :(?P<category>.*)   # Warning - can contain ":"'s
   :(?P<iow>[^:]+)
   :(?P<pe_taskid>[^:]+)
   :(?P<maxvmem>[^:]+)
   :(?P<arid>[^:]+)
   :(?P<ar_sub_time>[^:]+)
   $
""", re.VERBOSE)

alloc_def = re.compile(r"""
   sgealloc
   \s+cluster=\S+
   \s+job=(?P<job_number>\d+)\.(?P<task_number>\S+)
   \s+(?P<alloc>\S+)
""", re.VERBOSE)

host_def = re.compile(r"""
   \S+@(\S+)=\d+
""", re.VERBOSE)

host_prune = re.compile(r"[^.]+")

number_suffix_def = re.compile(r"^([0-9.]+)(\D+)$")
number_time_def   = re.compile(r"^(\d+):(\d+):(\d+)$")

node_type_def = re.compile(r"""
   ^(?P<num_pe>\d+)
   (?P<pe_type>core|thread)-
   (?P<memory>[^-]+)
   (-(?P<coproc>[^-]+))?
""", re.VERBOSE)


# Generator
# Walks all accounting records, returning a dictionary per record
# Allows retrieval of all records, or just one at a time.
def records(accounting = os.environ["SGE_ROOT"] +
                        "/" +
                        os.environ["SGE_CELL"] +
                        "/common/accounting",
            filter = None,
          ):
   for line in open(accounting):
      r = record_def.match(line)
      if r:
         d = r.groupdict()

         # Create a combined job/task name
         d['name'] = d['job_number'] + "." + ('1' if d['task_number'] == '0' else d['task_number'])

         # Prune DNS domainname (most SGE installations are domainname-insensitive)
         d['hostname'] = host_prune.match(d['hostname']).group()

         # Convert integer fields from strings to integers
         for f in [
                     'job_number',
                     'submission_time',
                     'start_time',
                     'end_time',
                     'failed',
                     'exit_status',
                     'slots',
                     'task_number',
                     'arid',
                     'ar_sub_time',
                  ]:
            d[f] = int(d[f])

         # Convert float fields from strings to floats
         for f in [
                     'priority',
                     'ru_wallclock',
                     'ru_utime',
                     'ru_stime',
                     'ru_maxrss',
                     'ru_ixrss',
                     'ru_ismrss',
                     'ru_idrss',
                     'ru_isrss',
                     'ru_minflt',
                     'ru_majflt',
                     'ru_nswap',
                     'ru_inblock',
                     'ru_oublock',
                     'ru_msgsnd',
                     'ru_msgrcv',
                     'ru_nsignals',
                     'ru_nvcsw',
                     'ru_nivcsw',
                     'cpu',
                     'mem',
                     'io',
                     'iow',
                     'maxvmem',
                  ]:
            d[f] = float(d[f])

         # Filter out undesirable records
         if filter:
            if not filter(d): continue

         # Return record
         yield(d)


# Generator
# Walks all job compute node allocation records, returning a dictionary per record
# Allows retrieval of all records, or just one at a time.
def allocs(allocs = "/var/log/local2"):
   for line in open(allocs):
      r = alloc_def.search(line)
      if r:
         d = r.groupdict()
         d['hosts'] = [ host_def.search(h).group(1) for h in d['alloc'].split(",") ]
         d['name'] = d['job_number'] + "." + d['task_number']
         yield(d)


# Expand a number potentially using gridengine numeric suffixes to a
# simple integer
def number(num):
   # Suffix to expand?
   r = number_suffix_def.match(str(num))
   if r:
      for e in enumerate(["K", "M", "G", "T"], start=1):
         if e[1] == r.group(2): return int(float(r.group(1))*1024**int(e[0]))

      for e in enumerate(["k", "m", "g", "t"], start=1):
         if e[1] == r.group(2): return int(float(r.group(1))*1000**int(e[0]))

   # Time to expand?
   r = number_time_def.match(str(num))
   if r:
      return int(r.group(1))*3600 + int(r.group(2))*60 + int(r.group(3))

   return int(num)

#DEBUG - not working yet. Will simplify an integer using gridengine
# size suffixes.
def contract_number(num):
   True

#DEBUG - not working yet. Initial attempt to migrate some perl code for
# contract_number - rethink this in python.
# WARNING: python2 division treat this differently?
def find_suffix(num, base):
   val = num / base
   print("test",val, num // base)
   if val != 0 and val == (num // base):
      print("recurse...")
      val, nbase = find_suffix(val, base)
      print("2",val,nbase)
      return val, nbase+1
   else:
      return val, 0

# When supplied an accounting "category" string, return a specified
# resource request
def category_resource(category, resource):
   swtch = False
   for c in category.split(' '):
      if swtch:
         # Extract the resource request
         for r in c.split(','):
            d = r.split('=')
            if d[0] == resource:
               if d[0] == 'h_vmem' or d[0] == 'h_rt':
                  return number(d[1])
               else:
                  return d[1]
         swtch = False

      # Find the start of a resource request string
      if c == '-l': swtch = True

   return 0

# When supplied a node_type string, extract a given component
# (num_pe, pe_type, memory, coproc)
def node_type(nodetype, element):
   r = node_type_def.match(nodetype)
   if r:
      d = r.groupdict()
      if element in d:
         return d[element]

