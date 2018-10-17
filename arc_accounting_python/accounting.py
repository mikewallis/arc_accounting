#!/bin/env python

# Accounting file parser, to answer questions such as:
# - What is the breakdown of usage between faculties?
# - What is the breakdown of usage between users?
# - What is the breakdown of usage between users within a faculty?

# Try and be python2 compatible
from __future__ import print_function

import argparse
import os
import re
import sys
import math
import sge
import datetime
import time
import pytz

from tabulate import tabulate
from functools import reduce
from dateutil.relativedelta import relativedelta

# Command line arguments
# ----------------------

parser = argparse.ArgumentParser(description='Report on accounting data')
parser.add_argument('--dates', action='store', type=str, help="Date range in UTC to report on, format [DATE][-[DATE]] where DATE has format YYYY[MM[DD[HH[MM[SS]]]]] e.g. 2018 for that year, 2018-2019 for two years, -2018 for everything up to the start of 2018, 2018- for everything after the start of 2018, 201803 for March 2018, 201806-201905 for 12 months starting June 2018. Multiple ranges supported.")
parser.add_argument('--skipqueues', action='append', type=str, help="Queue(s) to filter out")
parser.add_argument('--queues', action='append', type=str, help="Queue(s) to report on")
parser.add_argument('--users', action='append', type=str, help="Users(s) to report on")
parser.add_argument('--skipusers', action='append', type=str, help="Users(s) to filter out")
parser.add_argument('--projects', action='append', type=str, help="Project(s) to report on")
parser.add_argument('--skipprojects', action='append', type=str, help="Project(s) to filter out")
parser.add_argument('--parents', action='append', type=str, help="Project parent(s) to report on")
parser.add_argument('--skipparents', action='append', type=str, help="Project parent(s) to filter out")
parser.add_argument('--coreprojects', action='store_true', default=False, help="Report on the core set of projects")
parser.add_argument('--limitusers', action='store', type=int, default=sys.maxsize, help="Report on n most significant users")
parser.add_argument('--accountingfile', action='append', type=str, help="Read accounting data from file")
parser.add_argument('--cores', action='store', default=0, type=int, help="Total number of cores to calculate utilisation percentages from")
parser.add_argument('--reports', action='append', type=str, help="What information to report on (default: totalsbydate, projects, users, projectbyusers)")
parser.add_argument('--sizebins', action='append', type=str, help="Job size range to report statistics on, format [START][-[END]]. Multiple ranges supported.")
parser.add_argument('--noadjust', action='store_true', default=False, help="Do not adjust core hours to account for memory utilisation")
parser.add_argument('--nocommas', action='store_true', default=False, help="Do not add thousand separators in tables")
parser.add_argument('--printrecords', action='store_true', default=False, help="Print records to standard out")

args = parser.parse_args()

# Prepare regexes
# ---------------

range_def = re.compile(r"^(\d+)?(-(\d+)?)?$")

datetime_def = re.compile(r"^(\d{4})(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?$")

project_def = re.compile(r"^([a-z]+_)?(\S+)")

# Init parameters
# ---------------

# Maximum date (YYYY[MM[DD[HH[MM[SS]]]]]) or number to report on
max_date = "40000101"
max_num = sys.maxsize -1

# Backup method of determining node memory per core (mpc), in absence of
# node_type in job record, from hostname
backup_node_mpc = [
   { 'regex': r"^h7s3b1[56]", 'mpc': sge.number("64G") // 24 }, # ARC2
   { 'regex': r"^h[12367]s",  'mpc': sge.number("24G") // 12 }, # ARC2
   { 'regex': r"^dc[1-4]s",   'mpc': sge.number("128G") // 24 }, # ARC3
   { 'regex': r"^c2s0b[0-3]n",'mpc': sge.number("24G") // 8 }, # ARC1
   { 'regex': r"^c[1-3]s",    'mpc': sge.number("12G") // 8 }, # ARC1
   { 'regex': r"^smp[1-4]",   'mpc': sge.number("128G") // 16 }, # ARC1
   { 'regex': r"^g8s([789]|10)n", 'mpc': sge.number("256G") // 16 }, # POLARIS
   { 'regex': r"^g[0-9]s",    'mpc': sge.number("64G") // 16 }, # POLARIS/ARC2
   { 'regex': r"^hb01s",      'mpc': sge.number("256G") // 20 }, # MARC1
   { 'regex': r"^hb02n",      'mpc': sge.number("3T") // 48 }, # MARC1
]

# Compile regexes
for n in backup_node_mpc:
   n['re'] = re.compile(n['regex'])

# Some jobs weren't allocated to a project and should have been: use the
# queue name to do this retrospectively
queue_project_mapping = {
   'env1_sgpc.q': 'sgpc',
   'env1_glomap.q': 'glomap',
   'speme1.q': 'speme',
   'env1_neiss.q': 'neiss',
   'env1_tomcat.q': 'tomcat',
   'chem1.q': 'chem',
   'civ1.q': 'civil',
   'mhd1.q': 'mhd',
   'palaeo1.q': 'palaeo1',
}

# Parent of project mappings
# (if not in table, assumes project is own parent)
project_parent_regex = [
   { 'regex': r'^(minphys|glocat|glomap|tomcat|palaeo1|sgpc|neiss)$', 'parent': 'ENV' },
   { 'regex': r'^(speme|civil)$', 'parent': 'ENG' },
   { 'regex': r'^(mhd|skyblue|chem|maths|astro|codita)$', 'parent': 'MAPS' },
   { 'regex': r'^(omics|cryoem)$', 'parent': 'FBS' },

   { 'regex': r'^N8HPC_DUR_', 'parent': 'DUR' },
   { 'regex': r'^N8HPC_LAN_', 'parent': 'LAN' },
   { 'regex': r'^N8HPC_LDS_', 'parent': 'LDS' },
   { 'regex': r'^N8HPC_LIV_', 'parent': 'LIV' },
   { 'regex': r'^N8HPC_MCR_', 'parent': 'MCR' },
   { 'regex': r'^N8HPC_NCL_', 'parent': 'NCL' },
   { 'regex': r'^N8HPC_SHE_', 'parent': 'SHE' },
   { 'regex': r'^N8HPC_YRK_', 'parent': 'YRK' },
]

# Compile regexes
for n in project_parent_regex:
   n['re'] = re.compile(n['regex'])

# Some projects have changed names, or combined with other
# projects over the years. Combine them by updating old names.
project_project_mapping = {
   'ISS': 'ARC',
   'UKMHD': 'MAPS',
}

# Routines
# --------

def main():
   # One date range for all time, if not specified
   if not args.dates:
      args.dates = [ '-' ]

   args.dates = commasep_list(args.dates)

   # Restrict to the core purchasers of ARC, if requested
   if args.coreprojects:
      args.projects = [ 'Arts', 'ENG', 'ENV', 'ESSL', 'FBS', 'LUBS', 'MAPS', 'MEDH', 'PVAC' ]

   # Read default accounting file if none specified
   if not args.accountingfile:
      args.accountingfile = [ os.environ["SGE_ROOT"] + "/" + os.environ["SGE_CELL"] + "/common/accounting" ]

   # All reports, if not specified
   if not args.reports:
      if len(args.dates) > 1:
         args.reports = [ 'totalsbydate', 'projectsbydate', 'usersbydate' ]
      else:
         args.reports = [ 'projects', 'users', 'projectbyusers' ]

   # Job size bins, if not specified
   if not args.sizebins:
      args.sizebins = [ '1', '2-24', '25-48', '49-128', '129-256', '257-512', '513-10240' ]

   # Allow comma separated values to indicate arrays
   #DEBUG - would be better as a custom parseargs action
   args.skipqueues = commasep_list(args.skipqueues)
   args.queues = commasep_list(args.queues)
   args.users = commasep_list(args.users)
   args.skipusers = commasep_list(args.skipusers)
   args.projects = commasep_list(args.projects)
   args.skipprojects = commasep_list(args.skipprojects)
   args.parents = commasep_list(args.parents)
   args.skipparents = commasep_list(args.skipparents)
   args.accountingfile = commasep_list(args.accountingfile)
   args.reports = commasep_list(args.reports)
   args.sizebins = commasep_list(args.sizebins)

   # Parse date argument
   global dates
   dates = parse_startend(args.dates)

   # Parse job size bins
   sizebins = parse_startend(args.sizebins, type='int')

   # Initialise our main data structure
   data = [ { 'date': d, 'projusers': {}, 'users': {}, 'projects': {} } for d in dates]

   # Collect raw data, split by project and user
   for accounting in args.accountingfile:
      for record in sge.records(accounting=accounting, modify=record_modify):
         for d in data:
            if record_filter(record, d['date']):
               user = record['owner']
               project = record['project']

               # Print record, if requested
               if args.printrecords:
                  for f in sorted(record):
                     print(f +":", record[f])
                  print("\n")

               # Init data

               if project not in d['projusers']:
                  d['projusers'][project] = {}

               if user not in d['projusers'][project]:
                  d['projusers'][project][user] = {
                     'jobs': 0,
                     'core_hours': 0,
                     'core_hours_adj': 0,
                     'cpu_hours': 0,
                     'mem_hours': 0,
                     'mem_req_hours': 0,
                     'wait_hours': 0,
                     'job_size': [0 for b in sizebins],
                  }

               # Record usage

               # - count jobs
               d['projusers'][project][user]['jobs'] += 1

               # - count blocked core hours
               d['projusers'][project][user]['core_hours'] += record['core_hours']
               d['projusers'][project][user]['core_hours_adj'] += record['core_hours_adj']

               # - count used core hours
               d['projusers'][project][user]['cpu_hours'] += record['cpu'] / float(3600)

               # - count used and blocked memory
               d['projusers'][project][user]['mem_hours'] += record['core_hours'] * record['maxvmem']
               d['projusers'][project][user]['mem_req_hours'] += record['core_hours'] * record['mem_req']

               # - count wait time
               d['projusers'][project][user]['wait_hours'] += max((record['end_time'] - record['submission_time']) / float(3600), 0)

               # - job size distribution
               for (i, b) in enumerate(sizebins):
                  if record['job_size_adj'] >= b['start'] and record['job_size_adj'] < b['end']:
                     d['projusers'][project][user]['job_size'][i] += record['core_hours_adj']


   # Create summary info for projects and users
   for d in data:
      # Store info derived from date range
      ##DEBUG - figure out what to do here when range is for all time
      d['date']['hours'] = (d['date']['end'] - d['date']['start'])/float(3600)
      d['date']['core_hours'] = d['date']['hours'] * args.cores

      # Aggregate info for each user
      for project in d['projusers']:
         for user in d['projusers'][project]:
            if user not in d['users']:
               d['users'][user] = {
                  'jobs': 0,
                  'core_hours': 0,
                  'core_hours_adj': 0,
                  'cpu_hours': 0,
                  'mem_hours': 0,
                  'mem_req_hours': 0,
                  'wait_hours': 0,
                  'job_size': [0 for b in sizebins],
               }

            d['users'][user]['jobs'] += d['projusers'][project][user]['jobs']
            d['users'][user]['core_hours'] += d['projusers'][project][user]['core_hours']
            d['users'][user]['core_hours_adj'] += d['projusers'][project][user]['core_hours_adj']
            d['users'][user]['cpu_hours'] += d['projusers'][project][user]['cpu_hours']
            d['users'][user]['mem_hours'] += d['projusers'][project][user]['mem_hours']
            d['users'][user]['mem_req_hours'] += d['projusers'][project][user]['mem_req_hours']
            d['users'][user]['wait_hours'] += d['projusers'][project][user]['wait_hours']

            for (i, b) in enumerate(sizebins):
               d['users'][user]['job_size'][i] += d['projusers'][project][user]['job_size'][i]


      # Aggregate info for each project
      for project, dat in d['projusers'].items():
         d['projects'][project] = {
            'users': 0,
            'jobs': 0,
            'core_hours': 0,
            'core_hours_adj': 0,
            'cpu_hours': 0,
            'mem_hours': 0,
            'mem_req_hours': 0,
            'wait_hours': 0,
            'job_size': [0 for b in sizebins],
         }

         for user in dat.values():
            d['projects'][project]['users'] += 1
            d['projects'][project]['jobs'] += user['jobs']
            d['projects'][project]['core_hours'] += user['core_hours']
            d['projects'][project]['core_hours_adj'] += user['core_hours_adj']
            d['projects'][project]['cpu_hours'] += user['cpu_hours']
            d['projects'][project]['mem_hours'] += user['mem_hours']
            d['projects'][project]['mem_req_hours'] += user['mem_req_hours']
            d['projects'][project]['wait_hours'] += user['wait_hours']

            for (i, b) in enumerate(sizebins):
               d['projects'][project]['job_size'][i] += user['job_size'][i]


   # Spit out answer
   print_summary(data, args.cores, args.reports, sizebins)


def record_filter(record, date):
   # - Time filtering
   if record['end_time'] < date['start'] or record['start_time'] >= date['end']: return False

   # - Queue filtering
   if args.skipqueues and record['qname'] in args.skipqueues: return False
   if args.queues and record['qname'] not in args.queues: return False

   # - User filtering
   if args.skipusers and record['owner'] in args.skipusers: return False
   if args.users and record['owner'] not in args.users: return False

   # - Project filtering
   if args.skipprojects and record['project'] in args.skipprojects: return False
   if args.projects and record['project'] not in args.projects: return False

   # - Project parent filtering
   if args.skipparents and record['parent'] in args.skipparents: return False
   if args.parents and record['parent'] not in args.parents: return False

   return True


def record_modify(record):

   # Tweak project

   r = project_def.match(record['project'])
   if r:
      project = r.group(2)

      # - queue to project mapping
      if record['qname'] in queue_project_mapping:
         project = queue_project_mapping[record['qname']]

      # - project to project mapping (name changes, mergers, etc.)
      if project in project_project_mapping:
         project = project_project_mapping[project]
   else:
      project = '<unknown>'

   record['project'] = project

   # Add project parent
   record['parent'] = project_to_parent(project)

   # Add size and core hour figures

   if args.noadjust:
      size_adj = float(1)
   else:
      size_adj = return_size_adj(record)

   record['job_size'] = record['slots']
   record['job_size_adj'] = record['slots'] * size_adj

   record['core_hours'] = record['ru_wallclock'] * record['job_size'] / float(3600)
   record['core_hours_adj'] = record['ru_wallclock'] * record['job_size_adj'] / float(3600)

   # Add memory requested figure
   record['mem_req'] = record['slots'] * sge.category_resource(record['category'], 'h_vmem')


# Calculate effective job size multiplier
def return_size_adj(record):
   # - obtain node memory per core
   mem_core = None
   nt = sge.category_resource(record['category'], 'node_type')
   if nt:
      cores  = sge.number(sge.node_type(nt, 'num_pe'))
      memory = sge.number(sge.node_type(nt, 'memory'))

      if cores and memory:
         mem_core = memory // cores

   # - backup method of figuring out node memory per core
   if not mem_core:
      # Cycle through node name regexs for a match
      for b in backup_node_mpc:
         r = b['re'].match(record['hostname'])
         if r:
            mem_core = b['mpc']

   # - obtain memory request
   mem_req = sge.category_resource(record['category'], 'h_vmem')
   if mem_req:
      mem_req = sge.number(mem_req)

   size_adj = float(1)

   if mem_req is not None and mem_core is not None:
      size_adj = math.ceil(mem_req / float(mem_core))
      #size_adj max(1, mem_req / float(mem_core))
   else:
      print("Warning: could not extract mem or mem per node details for", record['name'],"("+record['category']+")", file=sys.stderr)

   return size_adj


def summarise_totalsbydate(data, total_cores, bins):
   headers = [ 'Date', 'Projects', 'Users', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', 'Core Hrs/Wait', 'Core %Eff', 'Mem %Eff' ]
   if bins:
      headers.extend([b['name'] for b in bins])

   avail_core_hours = sum([d['date']['core_hours'] for d in data])
   total_cpu_hours = 0
   total_mem_hours = 0
   total_mem_req_hours = 0
   total_wait_hours = 0

   table = []
   for d in data:
      core_hours_adj = sum([d['projects'][p]['core_hours_adj'] for p in d['projects']])

      cpu_hours = sum([d['projects'][p]['cpu_hours'] for p in d['projects']])
      total_cpu_hours += cpu_hours

      mem_hours = sum([d['projects'][p]['mem_hours'] for p in d['projects']])
      total_mem_hours += mem_hours

      mem_req_hours = sum([d['projects'][p]['mem_req_hours'] for p in d['projects']])
      total_mem_req_hours += mem_req_hours

      wait_hours = sum([d['projects'][p]['wait_hours'] for p in d['projects']])
      total_wait_hours += wait_hours

      table.append({
         'Date': d['date']['name'],
         'Projects': len(d['projects']),
         'Users': len(d['users']),
         'Jobs': sum([d['projects'][p]['jobs'] for p in d['projects']]),
         'Core Hrs': sum([d['projects'][p]['core_hours'] for p in d['projects']]),
         '%Utl': percent(sum([d['projects'][p]['core_hours'] for p in d['projects']]), d['date']['core_hours']),
         'Adj Core Hrs': sum([d['projects'][p]['core_hours_adj'] for p in d['projects']]),
         'Adj %Utl': percent(sum([d['projects'][p]['core_hours_adj'] for p in d['projects']]), d['date']['core_hours']),
         'Core Hrs/Wait': div(core_hours_adj, wait_hours),
         'Core %Eff': percent(cpu_hours,  core_hours_adj),
         'Mem %Eff': percent(mem_hours, mem_req_hours),
         **{ b['name']: sum([d['projects'][p]['job_size'][i] for p in d['projects']]) for i, b in enumerate(bins) },
      })

   totals = {
      'Date': 'TOTALS',
      'Projects': len(set([p for d in data for p in d['projusers']])),
      'Users': len(set([u for d in data for u in d['users']])),
      'Jobs': sum_key(table, 'Jobs'),
      'Core Hrs': sum_key(table, 'Core Hrs'),
      '%Utl': percent(sum_key(table, 'Core Hrs'), avail_core_hours),
      'Adj Core Hrs': sum_key(table, 'Adj Core Hrs'),
      'Adj %Utl': percent(sum_key(table, 'Adj Core Hrs'), avail_core_hours),
      'Core Hrs/Wait': div(sum_key(table, 'Adj Core Hrs'), total_wait_hours),
      'Core %Eff': percent(total_cpu_hours, sum_key(table, 'Adj Core Hrs')),
      'Mem %Eff': percent(total_mem_hours, total_mem_req_hours),
      **{ b['name']: sum([d[b['name']] for d in table]) for i, b in enumerate(bins) },
   }

   return headers, table, totals


def summarise_projectsbydate(data, project, total_cores, bins):
   headers = [ 'Date', 'Users', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', 'Core Hrs/Wait', '%Usg', 'Core %Eff', 'Mem %Eff' ]
   if bins:
      headers.extend([b['name'] for b in bins])

   avail_core_hours = sum([d['date']['core_hours'] for d in data])
   total_core_hours_adj = 0
   total_cpu_hours = 0
   total_mem_hours = 0
   total_mem_req_hours = 0
   total_wait_hours = 0

   table = []
   for d in data:
      if project in d['projusers']:
         core_hours_adj = sum([d['projects'][p]['core_hours_adj'] for p in d['projects']])
         total_core_hours_adj += core_hours_adj

         total_cpu_hours += d['projects'][project]['cpu_hours']

         total_mem_hours += d['projects'][project]['mem_hours']
         total_mem_req_hours += d['projects'][project]['mem_req_hours']

         total_wait_hours += d['projects'][project]['wait_hours']
 
         table.append({
            'Date': d['date']['name'],
            'Users': d['projects'][project]['users'],
            'Jobs': d['projects'][project]['jobs'],
            'Core Hrs': d['projects'][project]['core_hours'],
            '%Utl': percent(d['projects'][project]['core_hours'], d['date']['core_hours']),
            'Adj Core Hrs': d['projects'][project]['core_hours_adj'],
            'Adj %Utl': percent(d['projects'][project]['core_hours_adj'], d['date']['core_hours']),
            'Core Hrs/Wait': div(d['projects'][project]['core_hours_adj'], d['projects'][project]['wait_hours']),
            '%Usg': percent(d['projects'][project]['core_hours_adj'], core_hours_adj),
            'Core %Eff': percent(d['projects'][project]['cpu_hours'], d['projects'][project]['core_hours_adj']),
            'Mem %Eff': percent(d['projects'][project]['mem_hours'], d['projects'][project]['mem_req_hours']),
            **{ b['name']: d['projects'][project]['job_size'][i] for i, b in enumerate(bins) },
         })
      else:
         table.append({
            'Date': d['date']['name'],
            'Users': 0,
            'Jobs': 0,
            'Core Hrs': 0,
            '%Utl': percent(0, 0),
            'Adj Core Hrs': 0,
            'Adj %Utl': percent(0, 0),
            'Core Hrs/Wait': 0,
            '%Usg': percent(0, 0),
            'Core %Eff': percent(0, 0),
            'Mem %Eff': percent(0, 0),
            **{ b['name']: 0 for i, b in enumerate(bins) },
         })

   totals = {
      'Date': 'TOTALS',
      'Users': len(set([u for d in data for u in d['projusers'].get(project, [])])),
      'Jobs': sum_key(table, 'Jobs'),
      'Core Hrs': sum_key(table, 'Core Hrs'),
      '%Utl': percent(sum_key(table, 'Core Hrs'), avail_core_hours),
      'Adj Core Hrs': sum_key(table, 'Adj Core Hrs'),
      'Adj %Utl': percent(sum_key(table, 'Adj Core Hrs'), avail_core_hours),
      'Core Hrs/Wait': div(sum_key(table, 'Adj Core Hrs'), total_wait_hours),
      '%Usg': percent(sum_key(table, 'Adj Core Hrs'), total_core_hours_adj),
      'Core %Eff': percent(total_cpu_hours, sum_key(table, 'Adj Core Hrs')),
      'Mem %Eff': percent(total_mem_hours, total_mem_req_hours),
      **{ b['name']: sum([d[b['name']] for d in table]) for i, b in enumerate(bins) },
   }

   return headers, table, totals


def summarise_usersbydate(data, user, total_cores, bins):
   headers = [ 'Date', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', 'Core Hrs/Wait', '%Usg', 'Core %Eff', 'Mem %Eff' ]
   if bins:
      headers.extend([b['name'] for b in bins])

   avail_core_hours = sum([d['date']['core_hours'] for d in data])
   total_core_hours_adj = 0
   total_cpu_hours = 0
   total_mem_hours = 0
   total_mem_req_hours = 0
   total_wait_hours = 0

   table = []
   for d in data:
      if user in d['users']:
         core_hours_adj = sum([d['users'][u]['core_hours_adj'] for u in d['users']])
         total_core_hours_adj += core_hours_adj

         total_cpu_hours += d['users'][user]['cpu_hours']

         total_mem_hours += d['users'][user]['mem_hours']
         total_mem_req_hours += d['users'][user]['mem_req_hours']

         total_wait_hours += d['users'][user]['wait_hours']
 
         table.append({
            'Date': d['date']['name'],
            'Jobs': d['users'][user]['jobs'],
            'Core Hrs': d['users'][user]['core_hours'],
            '%Utl': percent(d['users'][user]['core_hours'], d['date']['core_hours']),
            'Adj Core Hrs': d['users'][user]['core_hours_adj'],
            'Adj %Utl': percent(d['users'][user]['core_hours_adj'], d['date']['core_hours']),
            'Core Hrs/Wait': div(d['users'][user]['core_hours_adj'], d['users'][user]['wait_hours']),
            '%Usg': percent(d['users'][user]['core_hours_adj'], core_hours_adj),
            'Core %Eff': percent(d['users'][user]['cpu_hours'], d['users'][user]['core_hours_adj']),
            'Mem %Eff': percent(d['users'][user]['mem_hours'], d['users'][user]['mem_req_hours']),
            **{ b['name']: d['users'][user]['job_size'][i] for i, b in enumerate(bins) },
         })
      else:
         table.append({
            'Date': d['date']['name'],
            'Jobs': 0,
            'Core Hrs': 0,
            '%Utl': percent(0, 0),
            'Adj Core Hrs': 0,
            'Adj %Utl': percent(0, 0),
            'Core Hrs/Wait': 0,
            '%Usg': percent(0, 0),
            'Core %Eff': percent(0, 0),
            'Mem %Eff': percent(0, 0),
            **{ b['name']: 0 for i, b in enumerate(bins) },
         })

   totals = {
      'Date': 'TOTALS',
      'Jobs': sum_key(table, 'Jobs'),
      'Core Hrs': sum_key(table, 'Core Hrs'),
      '%Utl': percent(sum_key(table, 'Core Hrs'), avail_core_hours),
      'Adj Core Hrs': sum_key(table, 'Adj Core Hrs'),
      'Adj %Utl': percent(sum_key(table, 'Adj Core Hrs'), avail_core_hours),
      'Core Hrs/Wait': div(sum_key(table, 'Adj Core Hrs'), total_wait_hours),
      '%Usg': percent(sum_key(table, 'Adj Core Hrs'), total_core_hours_adj),
      'Core %Eff': percent(total_cpu_hours, sum_key(table, 'Adj Core Hrs')),
      'Mem %Eff': percent(total_mem_hours, total_mem_req_hours),
      **{ b['name']: sum([d[b['name']] for d in table]) for i, b in enumerate(bins) },
   }

   return headers, table, totals


def summarise_projects(data, total_cores, bins):
   headers = [ 'Project', 'Parent', 'Users', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', 'Core Hrs/Wait', '%Usg', 'Core %Eff', 'Mem %Eff' ]
   if bins:
      headers.extend([b['name'] for b in bins])

   core_hours_adj = sum([data['projects'][p]['core_hours_adj'] for p in data['projects']])

   table = []
   for project, d in sorted(data['projects'].items(), key=lambda item: item[1]['core_hours_adj'], reverse=True):
      table.append({
         'Project': project,
         'Parent': project_to_parent(project),
         'Users': d['users'],
         'Jobs': d['jobs'],
         'Core Hrs': d['core_hours'],
         '%Utl': percent(d['core_hours'], data['date']['core_hours']),
         'Adj Core Hrs': d['core_hours_adj'],
         'Adj %Utl': percent(d['core_hours_adj'], data['date']['core_hours']),
         'Core Hrs/Wait': div(d['core_hours_adj'], d['wait_hours']),
         '%Usg': percent(d['core_hours_adj'], core_hours_adj),
         'Core %Eff': percent(d['cpu_hours'], d['core_hours_adj']),
         'Mem %Eff': percent(d['mem_hours'], d['mem_req_hours']),
         **{ b['name']: d['job_size'][i] for i, b in enumerate(bins) },
      }),

   totals = {
      'Project': 'TOTALS',
      'Parent': '-',
      'Users': len(data['users']),
      'Jobs': sum_key(table, 'Jobs'),
      'Core Hrs': sum_key(table, 'Core Hrs'),
      '%Utl': percent(sum_key(table, 'Core Hrs'), data['date']['core_hours']),
      'Adj Core Hrs': sum_key(table, 'Adj Core Hrs'),
      'Adj %Utl': percent(sum_key(table, 'Adj Core Hrs'), data['date']['core_hours']),
      'Core Hrs/Wait': div(sum_key(table, 'Adj Core Hrs'), sum_key(table, 'Core Hrs/Wait')),
      '%Usg': percent(sum_key(table, 'Adj Core Hrs'), core_hours_adj),
      'Core %Eff': percent(sum([data['projects'][p]['cpu_hours'] for p in data['projects']]), sum_key(table, 'Adj Core Hrs')),
      'Mem %Eff': percent(sum([data['projects'][p]['mem_hours'] for p in data['projects']]), sum([data['projects'][p]['mem_req_hours'] for p in data['projects']])),
      **{ b['name']: sum([d[b['name']] for d in table]) for i, b in enumerate(bins) },
   }

   return headers, table, totals


def summarise_users(data, total_cores, bins):
   headers = [ 'Usr', 'Project(s)', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', 'Core Hrs/Wait', '%Usg', 'Core %Eff', 'Mem %Eff' ]
   if bins:
      headers.extend([b['name'] for b in bins])

   core_hours_adj = sum([data['users'][u]['core_hours_adj'] for u in data['users']])

   table = []
   count = 0
   for user, d in sorted(data['users'].items(), key=lambda item: item[1]['core_hours_adj'], reverse=True):
      count += 1
      if count > args.limitusers: break
      table.append({
         'Usr': user,
         'Project(s)': ",".join(sorted([o for o in data['projusers'] for u in data['projusers'][o] if u == user])),
         'Jobs': d['jobs'],
         'Core Hrs': d['core_hours'],
         '%Utl': percent(d['core_hours'], data['date']['core_hours']),
         'Adj Core Hrs': d['core_hours_adj'],
         'Adj %Utl': percent(d['core_hours_adj'], data['date']['core_hours']),
         'Core Hrs/Wait': div(d['core_hours_adj'], d['wait_hours']),
         '%Usg': percent(d['core_hours_adj'], core_hours_adj),
         'Core %Eff': percent(d['cpu_hours'], d['core_hours_adj']),
         'Mem %Eff': percent(d['mem_hours'], d['mem_req_hours']),
         **{ b['name']: d['job_size'][i] for i, b in enumerate(bins) },
      })

   totals = {
      'Usr': 'TOTALS',
      'Project(s)': '-',
      'Jobs': sum_key(table, 'Jobs'),
      'Core Hrs': sum_key(table, 'Core Hrs'),
      '%Utl': percent(sum_key(table, 'Core Hrs'), data['date']['core_hours']),
      'Adj Core Hrs': sum_key(table, 'Adj Core Hrs'),
      'Adj %Utl': percent(sum_key(table, 'Adj Core Hrs'), data['date']['core_hours']),
      'Core Hrs/Wait': div(sum_key(table, 'Adj Core Hrs'), sum_key(table, 'Core Hrs/Wait')),
      '%Usg': percent(sum_key(table, 'Adj Core Hrs'), core_hours_adj),
      'Core %Eff': percent(sum([data['users'][u]['cpu_hours'] for u in data['users']]), sum_key(table, 'Adj Core Hrs')),
      'Mem %Eff': percent(sum([data['users'][u]['mem_hours'] for u in data['users']]), sum([data['users'][u]['mem_req_hours'] for u in data['users']])),
      **{ b['name']: sum([d[b['name']] for d in table]) for i, b in enumerate(bins) },
   }

   return headers, table, totals

def summarise_project(data, project, total_cores, bins):
   headers = [ 'Usr', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', 'Core Hrs/Wait', '%Usg', 'Core %Eff', 'Mem %Eff' ]
   if bins:
      headers.extend([b['name'] for b in bins])

   core_hours_adj = sum([data['projusers'][project][u]['core_hours_adj'] for u in data['projusers'][project]])

   table = []
   count = 0
   for user, d in sorted(data['projusers'][project].items(), key=lambda item: item[1]['core_hours_adj'], reverse=True):
      count += 1
      if count > args.limitusers: break
      table.append({
         'Usr': user,
         'Jobs': d['jobs'],
         'Core Hrs': d['core_hours'],
         '%Utl': percent(d['core_hours'], data['date']['core_hours']),
         'Adj Core Hrs': d['core_hours_adj'],
         'Adj %Utl': percent(d['core_hours_adj'], data['date']['core_hours']),
         'Core Hrs/Wait': div(d['core_hours_adj'], d['wait_hours']),
         '%Usg': percent(d['core_hours_adj'], core_hours_adj),
         'Core %Eff': percent(d['cpu_hours'], d['core_hours_adj']),
         'Mem %Eff': percent(d['mem_hours'], d['mem_req_hours']),
         **{ b['name']: d['job_size'][i] for i, b in enumerate(bins) },
      })

   totals = {
      'Usr': 'TOTALS',
      'Jobs': sum_key(table, 'Jobs'),
      'Core Hrs': sum_key(table, 'Core Hrs'),
      '%Utl': percent(sum_key(table, 'Core Hrs'), data['date']['core_hours']),
      'Adj Core Hrs': sum_key(table, 'Adj Core Hrs'),
      'Adj %Utl': percent(sum_key(table, 'Adj Core Hrs'), data['date']['core_hours']),
      'Core Hrs/Wait': div(sum_key(table, 'Adj Core Hrs'), sum_key(table, 'Core Hrs/Wait')),
      '%Usg': percent(sum_key(table, 'Adj Core Hrs'), core_hours_adj),
      'Core %Eff': percent(sum([data['projusers'][project][u]['cpu_hours'] for u in data['projusers'][project]]), sum_key(table, 'Adj Core Hrs')),
      'Mem %Eff': percent(sum([data['projusers'][project][u]['mem_hours'] for u in data['projusers'][project]]), sum([data['projusers'][project][u]['mem_req_hours'] for u in data['projusers'][project]])),
      **{ b['name']: sum([d[b['name']] for d in table]) for i, b in enumerate(bins) },
   }

   return headers, table, totals


def print_table(headers, data, totals):

   if len(headers) != len(set(headers)):
      print("ERROR: cannot have multiple columns with same name", headers)

   # Construct data for table
   tab_data = []
   for d in data:
      tab_data.append([d[column] for column in headers])

   if totals:
      tab_data.append([totals[column] for column in headers])

   # Attempt to promote all elements in table to floats,
   # in order to show thousands separator
   for row in tab_data:
      for (column, value) in enumerate(row):
         try:
            row[column] = float(value)
         except:
            None

   if args.nocommas:
      floatfmt=".0f"
   else:
      floatfmt=",.0f"

   print(tabulate(tab_data, headers=headers, floatfmt=floatfmt),"\n")


def print_summary(data, total_cores, reports, bins):

   if 'header' in reports:
      print("Accounting summary, reporting on jobs ending in the range(s):")
      for d in data:
         print(" Start:", time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(d['date']['start'])))
         print(" End:", time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(d['date']['end'])))
         print(" Duration:", (d['date']['end'] - d['date']['start'])//3600, "hours", "Cores:", total_cores)
         print("")

   if 'totalsbydate' in reports:
      print("=======")
      print("Totals:")
      print("=======\n")
      print_table(*summarise_totalsbydate(data, total_cores, bins))

   if 'projectsbydate' in reports:
      print("=================")
      print("Projects by date:")
      print("=================\n")
      for project in set([p for d in data for p in d['projusers']]):
         print("Project:", project)
         print_table(*summarise_projectsbydate(data, project, total_cores, bins))

   if 'projects' in reports:
      print("=============")
      print("Top projects:")
      print("=============\n")
      for d in data:
         print("Period:", d['date']['name'],"\n")
         print_table(*summarise_projects(d, total_cores, bins))

   if 'users' in reports:
      print("==========")
      print("Top users:")
      print("==========\n")
      for d in data:
         print("Period:", d['date']['name'],"\n")
         print_simplestats(d['users'], args.limitusers)
         print_table(*summarise_users(d, total_cores, bins))

   if 'usersbydate' in reports:
      print("=============")
      print("Users by date:")
      print("=============\n")
      for user in set([u for d in data for u in d['users']]):
         print("User:", user)
         print_table(*summarise_usersbydate(data, user, total_cores, bins))

   ##DEBUG - need a usersbyprojectbydate table?

   if 'projectbyusers' in reports:
      print("=====================")
      print("Top users by project:")
      print("=====================\n")

      for d in data:
         print("Period:", d['date']['name'],"\n")
         for project in sorted(d['projusers']):
            print("Project:", project)
            print_simplestats(d['projusers'][project], args.limitusers)
            print_table(*summarise_project(d, project, total_cores, bins))


def print_simplestats(data, top_n):
#   # Rewrite with reduce
#   top_usage = 0
#   for e in enumerate(sorted(data.items(), key=lambda item: item[1]['core_hours_adj'], reverse=True)):
#      if e[0] >= top_n: break
#      top_usage += e[1][1]['core_hours_adj']

#   bottom_usage = 0
#   bottom_n = 0
#   for e in enumerate(sorted(data.items(), key=lambda item: item[1]['core_hours_adj'])):
#      bottom_usage += e[1][1]['core_hours_adj']
#      if bottom_usage > top_usage:
#         bottom_n = max(0, e[0] -1)
#         break

#   if top_n <= len(data):
#      print(
#         len(data),"active users.",
#         "Top", top_n, "("+percent(top_n/len(data))+")",
#         "active users have used more than the bottom",
#         bottom_n, "("+percent(bottom_n/len(data))+")", "combined",
#      )
#   else:
#      print(len(data),"active users.", "Top", top_n, "skipped")

   print(len(data),"active users.")


# Calc and format percent, with safe division
def percent(num, dom):
   return "{0:.1%}".format(float(div(num,dom)))


# Safe division
def div(num, dom):
   return num / dom if dom else 0


# Shortcut to simplify calcs
def sum_key(data, key):
   return sum([d[key] for d in data])


# Take a range string of format [START][-[END]], where START and END are
# either integers, or dates of format YYYY[MM[DD[HH[MM[SS]]]]] in UTC.
#
# Return a list of dictionaries bounding the start and end of that range
# (start - inclusive, end - exclusive). If input were dates, return dates
# as seconds since the epoch.
def parse_startend(ranges, type='date'):
   d = []
   for range_str in ranges:
      start = 0
      end = max_num

      if type == 'date':
         end = int(datetime.datetime(
            *parse_date(max_date),
            tzinfo=pytz.timezone('UTC'),
         ).strftime('%s'))

      if range_str:
         r = range_def.match(range_str)
         if r:
            if type == 'date':
               if r.group(1):
                  start_dt = datetime.datetime(
                     *datetime_defaults(*parse_date(r.group(1))),
                     tzinfo=pytz.timezone('UTC'),
                  )

                  start = int(start_dt.strftime('%s'))

               end_dt = next_datetime(
                  *parse_date(r.group(3) or (r.group(2) and max_date) or r.group(1)),
                  tzinfo=pytz.timezone('UTC'),
               )

               end = int(end_dt.strftime('%s'))
            elif type == 'int':
               start = int(r.group(1) or 1)
               end = int(r.group(3) or (r.group(2) and max_num) or r.group(1)) +1

      d.append({ 'name': range_str, 'start': start, 'end': end })

   return d


# Take a date/time string with optional components of format
# YYYY[MM[DD[HH[MM[SS]]]]] and return that information split into a tuple
# as integers
def parse_date(date):
   if date:
      r = datetime_def.match(date)
      if r:
         # Convert strings to integers - don't initialise anything we don't
         # have information for.
         return ( int(e) for e in r.groups() if e != None )

   return None


# Takes similar arguments as datetime, returns a datetime
# object "1" louder, e.g. if args specify a particular month,
# will return the next month in the same year.
def next_datetime(*date_time, tzinfo=pytz.timezone('UTC')):
   t1 = datetime.datetime(*datetime_defaults(*date_time), tzinfo=tzinfo)

   case = {
      1: t1 + relativedelta(years=1),
      2: t1 + relativedelta(months=1),
      3: t1 + datetime.timedelta(days=1),
      4: t1 + datetime.timedelta(hours=1),
      5: t1 + datetime.timedelta(minutes=1),
      6: t1 + datetime.timedelta(seconds=1),
   }

   return case.get(len(date_time))


# Takes a list/tuple of datetime arguments (year, month, etc.), filling
# out with the minimum defaults assuming we're interested in the start
# of a month, year, or the Unix epoch.
def datetime_defaults(*date_time):
   t = list(date_time)

   # datetime needs some minimum information - apply defaults to any missing
   if len(t) < 1: t.append(1970) # year
   if len(t) < 2: t.append(1) # month
   if len(t) < 3: t.append(1) # day

   return tuple(t)


# Returns input expanded into a list, split
# as comma separate entries
def commasep_list(data):
   l = []

   if type(data) == type([]):
      for d in data:
         l.extend(d.split(","))
   elif data:
      l.extend(data.split(","))

   return l


def project_to_parent(project):
   for p in project_parent_regex:
      r = p['re'].match(project)
      if r: return p['parent']

   return project


# Run program (if we've not been imported)
# ---------------------------------------

if __name__ == "__main__":
   main()

