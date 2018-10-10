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
parser.add_argument('--date', action='store', type=str, help="Date range in UTC to report on, format [DATE][-[DATE]] where DATE has format YYYY[MM[DD[HH[MM[SS]]]]] e.g. 2018 for that year, 2018-2019 for two years, -2018 for everything up to the start of 2018, 2018- for everything after the start of 2018, 201803 for March 2018, 201806-201905 for 12 months starting June 2018")
parser.add_argument('--skipqueues', action='append', type=str, help="Queue to filter out")
parser.add_argument('--queues', action='append', type=str, help="Queue to report on")
parser.add_argument('--projects', action='append', type=str, help="Equipment project to report on")
parser.add_argument('--skipprojects', action='append', type=str, help="Equipment project to filter out")
parser.add_argument('--coreprojects', action='store_true', default=False, help="Report on core set of projects")
parser.add_argument('--limitusers', action='store', type=int, default=sys.maxsize, help="Report on n most significant users")
parser.add_argument('--accountingfile', action='append', type=str, help="Read accounting data from file")
parser.add_argument('--cores', action='store', default=0, type=int, help="Total number of cores to report utilisation on")
parser.add_argument('--reports', action='append', type=str, help="What information to report on (default: header, projects, users, usersbyproject)")
parser.add_argument('--sizebins', action='append', type=str, help="Job size range to report statistics on, format [START][-[END]]. Multiple ranges supported.")

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

# ARC1 accounting file needs a different method to distinguish
# equipment projects beyond the core projects
queue_project_mapping = {
   'env1_sgpc.q': 'sgpc',
   'env1_glomap.q': 'glomap',
   'speme1.q': 'speme',
   'env1_neiss.q': 'neiss',
   'env1_tomcat.q': 'tomcat',
   'chem1.q': 'chem',
   'civ1.q': 'civil',
   'mhd1.q': 'mhd',
}

# Parent of project mapping (e.g. mapping to core purchasers)
project_parent_mapping = {
   'ENV': 'ENV',
   'ENG': 'ENG',
   'MAPS': 'MAPS',
   'FBS': 'FBS',
   'ARC': 'ARC',
   'Arts': 'Arts',
   'LUBS': 'LUBS',
   'ESSL': 'ESSL',
   'PVAC': 'PVAC',
   'MEDH': 'MEDH',

   'minphys': 'ENV',
   'glocat': 'ENV',
   'glomap': 'ENV',
   'tomcat': 'ENV',
   'palaeo1': 'ENV',
   'sgpc': 'ENV',
   'neiss': 'ENV',

   'speme': 'ENG',
   'civil': 'ENG',

   'mhd': 'MAPS',
   'skyblue': 'MAPS',
   'chem': 'MAPS',
   'maths': 'MAPS',
   'astro': 'MAPS',
   'codita': 'MAPS',

   'omics': 'FBS',
   'cryoem': 'FBS',
}

# Some projects have changed names, or combined with other
# projects over the years.
project_project_mapping = {
   'ISS': 'ARC',
   'UKMHD': 'MAPS',
}

# Routines
# --------

def main():
   # Restrict to the core purchasers of ARC, if requested
   if args.coreprojects:
      args.projects = [ 'Arts', 'ENG', 'ENV', 'ESSL', 'FBS', 'LUBS', 'MAPS', 'MEDH', 'PVAC' ]

   # Read default accounting file if none specified
   if not args.accountingfile:
      args.accountingfile = [ os.environ["SGE_ROOT"] + "/" + os.environ["SGE_CELL"] + "/common/accounting" ]

   # All reports, if not specified
   if not args.reports:
      args.reports = [ 'header', 'totals', 'projects', 'users', 'usersbyproject' ]

   # Job size bins, if not specified
   if not args.sizebins:
      args.sizebins = [ '1', '2-24', '25-48', '49-128', '129-256', '257-512', '513-10240' ]

   sizebins = []
   for b in args.sizebins:
      start, end = parse_startend(b, type='int')
      sizebins.append({'name': b, 'start': start, 'end': end })

   # Allow comma separated values to indicate arrays
   #DEBUG - would be better as a custom parseargs action
   args.skipqueues = commasep_list(args.skipqueues)
   args.queues = commasep_list(args.queues)
   args.projects = commasep_list(args.projects)
   args.skipprojects = commasep_list(args.skipprojects)
   args.accountingfile = commasep_list(args.accountingfile)
   args.reports = commasep_list(args.reports)
   args.sizebins = commasep_list(args.sizebins)

   # Parse date argument
   global start_time, end_time
   start_time, end_time = parse_startend(args.date)

   # Collect raw data, split by project
   projects = {}
   for accounting in args.accountingfile:
      for record in sge.records(accounting=accounting, modify=record_modify, filter=record_filter):
         user = record['owner']
         project = record['equip_project'] # (refers to the equipment project)

         # - init data
         if project not in projects:
            projects[project] = {}

         if user not in projects[project]:
            projects[project][user] = {
               'jobs': 0,
               'core_hours': 0,
               'core_hours_adj': 0,
               'job_size': [0 for b in sizebins],
            }

         # - record usage
         projects[project][user]['jobs'] += 1

         projects[project][user]['core_hours'] += record['core_hours']
         projects[project][user]['core_hours_adj'] += record['core_hours_adj']

         for (i, b) in enumerate(sizebins):
            if record['job_size_adj'] >= b['start'] and record['job_size_adj'] < b['end']:
               projects[project][user]['job_size'][i] += record['core_hours_adj']

   # Calculate a summary for each user
   users = {}
   for project in projects:
      for user in projects[project]:
         if user not in users:
            users[user] = {
               'jobs': 0,
               'core_hours': 0,
               'core_hours_adj': 0,
               'job_size': [0 for b in sizebins],
            }

         users[user]['jobs'] += projects[project][user]['jobs']
         users[user]['core_hours'] += projects[project][user]['core_hours']
         users[user]['core_hours_adj'] += projects[project][user]['core_hours_adj']

         for (i, b) in enumerate(sizebins):
            users[user]['job_size'][i] += projects[project][user]['job_size'][i]


   # Calculate a summary for each project
   project_summaries = {}
   for project, data in projects.items():
      project_summaries[project] = {
         'users': 0,
         'jobs': 0,
         'core_hours': 0,
         'core_hours_adj': 0,
         'job_size': [0 for b in sizebins],
      }

      for user in data.values():
         project_summaries[project]['users'] += 1
         project_summaries[project]['jobs'] += user['jobs']
         project_summaries[project]['core_hours'] += user['core_hours']
         project_summaries[project]['core_hours_adj'] += user['core_hours_adj']

         for (i, b) in enumerate(sizebins):
            project_summaries[project]['job_size'][i] += user['job_size'][i]

   # Spit out answer
   print_summary(projects, users, project_summaries, args.cores, args.reports, sizebins)


def record_filter(record):
   # - Time filtering
   if record['end_time'] < start_time or record['start_time'] >= end_time: return False

   # - Queue filtering
   if args.skipqueues and record['qname'] in args.skipqueues: return False
   if args.queues and record['qname'] not in args.queues: return False

   # - Project filtering
   if args.skipprojects and record['equip_project'] in args.skipprojects: return False
   if args.projects and record['equip_project'] not in args.projects: return False

   return True


def record_modify(record):

   # Add record equipment project in record

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

   record['equip_project'] = project

   # Add size and core hour figures

   size_adj = return_size_adj(record)

   record['job_size'] = record['slots']
   record['job_size_adj'] = record['slots'] * size_adj

   record['core_hours'] = record['ru_wallclock'] * record['job_size'] / float(3600)
   record['core_hours_adj'] = record['ru_wallclock'] * record['job_size_adj'] / float(3600)


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


def summarise_totals(projects, users, project_summaries, inv_total_time, bins):
   h = [ 'Projects', 'Uniq Usrs', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl' ]
   if bins:
      h.extend([b['name'] for b in bins])

   d = [
      {
         'Projects': len(project_summaries),
         'Uniq Usrs': len(users),
         'Jobs': reduce((lambda x, k: x + project_summaries[k]['jobs']), project_summaries, 0),
         'Core Hrs': reduce((lambda x, k: x + project_summaries[k]['core_hours']), project_summaries, 0),
         '%Utl': percent(reduce((lambda x, k: x + project_summaries[k]['core_hours'] * inv_total_time), project_summaries, 0)),
         'Adj Core Hrs': reduce((lambda x, k: x + project_summaries[k]['core_hours_adj']), project_summaries, 0),
         'Adj %Utl': percent(reduce((lambda x, k: x + project_summaries[k]['core_hours_adj'] * inv_total_time), project_summaries, 0)),
         **{ b['name']: reduce((lambda x, k: x + project_summaries[k]['job_size'][i]), project_summaries, 0) for i, b in enumerate(bins) },
      },
   ]
   t = None

   return h, d, t


def summarise_projects(projects, users, project_summaries, inv_total_time, core_hours_adj, bins):
   h = [ 'Project', 'Parent', 'Uniq Usrs', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', '%Usg' ]
   if bins:
      h.extend([b['name'] for b in bins])

   d = []
   for project, data in sorted(project_summaries.items(), key=lambda item: item[1]['core_hours_adj'], reverse=True):
      d.append({
         'Project': project,
         'Parent': project_parent_mapping.get(project, '<unknown>'),
         'Uniq Usrs': data['users'],
         'Jobs': data['jobs'],
         'Core Hrs': round(data['core_hours']),
         '%Utl': percent(data['core_hours'] * inv_total_time),
         'Adj Core Hrs': round(data['core_hours_adj']),
         'Adj %Utl': percent(data['core_hours_adj'] * inv_total_time),
         '%Usg': percent(data['core_hours_adj'] / core_hours_adj),
         **{ b['name']: data['job_size'][i] for i, b in enumerate(bins) },
      })

   t = {
      'Project': 'TOTALS',
      'Parent': '-',
      'Uniq Usrs': len(users), # Note: unique users - not the sum of entries in column
      'Jobs': reduce((lambda x, k: x + project_summaries[k]['jobs']), project_summaries, 0),
      'Core Hrs': reduce((lambda x, k: x + project_summaries[k]['core_hours']), project_summaries, 0),
      '%Utl': percent(reduce((lambda x, k: x + project_summaries[k]['core_hours'] * inv_total_time), project_summaries, 0)),
      'Adj Core Hrs': reduce((lambda x, k: x + project_summaries[k]['core_hours_adj']), project_summaries, 0),
      'Adj %Utl': percent(reduce((lambda x, k: x + project_summaries[k]['core_hours_adj'] * inv_total_time), project_summaries, 0)),
      '%Usg': percent(reduce((lambda x, k: x + project_summaries[k]['core_hours_adj'] / core_hours_adj), project_summaries, 0)),
      **{ b['name']: reduce((lambda x, k: x + project_summaries[k]['job_size'][i]), project_summaries, 0) for i, b in enumerate(bins) },
   }

   return h, d, t


def summarise_users(projects, users, inv_total_time, core_hours_adj, bins):
   h = [ 'Usr', 'Project(s)', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', '%Usg' ]
   if bins:
      h.extend([b['name'] for b in bins])

   d = []
   count = 0
   for user, data in sorted(users.items(), key=lambda item: item[1]['core_hours_adj'], reverse=True):
      count += 1
      if count > args.limitusers: break
      d.append({
         'Usr': user,
         'Project(s)': ",".join(sorted([o  for o in projects for u in projects[o] if u == user])),
         'Jobs': data['jobs'],
         'Core Hrs': round(data['core_hours']),
         '%Utl': percent(data['core_hours'] * inv_total_time),
         'Adj Core Hrs': round(data['core_hours_adj']),
         'Adj %Utl': percent(data['core_hours_adj'] * inv_total_time),
         '%Usg': percent(data['core_hours_adj'] / core_hours_adj),
         **{ b['name']: data['job_size'][i] for i, b in enumerate(bins) },
      })

   t = {
      'Usr': 'TOTALS',
      'Project(s)': '-',
      'Jobs': reduce((lambda x, k: x + users[k]['jobs']), users, 0),
      'Core Hrs': reduce((lambda x, k: x + users[k]['core_hours']), users, 0),
      '%Utl': percent(reduce((lambda x, k: x + users[k]['core_hours'] * inv_total_time), users, 0)),
      'Adj Core Hrs': reduce((lambda x, k: x + users[k]['core_hours_adj']), users, 0),
      'Adj %Utl': percent(reduce((lambda x, k: x + users[k]['core_hours_adj'] * inv_total_time), users, 0)),
      '%Usg': percent(reduce((lambda x, k: x + users[k]['core_hours_adj'] / core_hours_adj), users, 0)),
      **{ b['name']: reduce((lambda x, k: x + users[k]['job_size'][i]), users, 0) for i, b in enumerate(bins) },
   }

   return h, d, t

def summarise_project(project, inv_total_time, core_hours_adj, bins):
   h = [ 'Usr', 'Jobs', 'Core Hrs', '%Utl', 'Adj Core Hrs', 'Adj %Utl', '%Usg' ]
   if bins:
      h.extend([b['name'] for b in bins])

   d = []
   count = 0
   for user, data in sorted(project.items(), key=lambda item: item[1]['core_hours_adj'], reverse=True):
      count += 1
      if count > args.limitusers: break
      d.append({
         'Usr': user,
         'Jobs': data['jobs'],
         'Core Hrs': round(data['core_hours']),
         '%Utl': percent(data['core_hours'] * inv_total_time),
         'Adj Core Hrs': round(data['core_hours_adj']),
         'Adj %Utl': percent(data['core_hours_adj'] * inv_total_time),
         '%Usg': percent(data['core_hours_adj'] / core_hours_adj),
         **{ b['name']: data['job_size'][i] for i, b in enumerate(bins) },
      })

   t = {
      'Usr': 'TOTALS',
      'Jobs': reduce((lambda x, k: x + project[k]['jobs']), project, 0),
      'Core Hrs': reduce((lambda x, k: x + project[k]['core_hours']), project, 0),
      '%Utl': percent(reduce((lambda x, k: x + project[k]['core_hours'] * inv_total_time), project, 0)),
      'Adj Core Hrs': reduce((lambda x, k: x + project[k]['core_hours_adj']), project, 0),
      'Adj %Utl': percent(reduce((lambda x, k: x + project[k]['core_hours_adj'] * inv_total_time), project, 0)),
      '%Usg': percent(reduce((lambda x, k: x + project[k]['core_hours_adj'] / core_hours_adj), project, 0)),
      **{ b['name']: reduce((lambda x, k: x + project[k]['job_size'][i]), project, 0) for i, b in enumerate(bins) },
   }

   return h, d, t


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

   print(tabulate(tab_data, headers=headers, floatfmt=",.0f"),"\n")


def print_summary(projects, users, project_summaries, total_cores, reports, bins):

   if 'header' in reports:
      print("Accounting summary, reporting on jobs ending in the range:")
      print(" Start:", time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(start_time)))
      print(" End:", time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(end_time)))
      print(" Duration:", (end_time - start_time)//3600, "hours", "Cores:", total_cores)
      print("")

   # Note: lots of promoting ints to floats for two reasons:
   # - make divisions behave the same on python2
   # - tabulate only provides the option to separate thousands for floats

   ##DEBUG - figure out what to do here when range is for all time
   if total_cores:
      inv_total_time = 1/float(((end_time - start_time)/float(3600)) * total_cores)
   else:
      inv_total_time = 0

   core_hours_adj = reduce((lambda x, k: x + project_summaries[k]['core_hours_adj']), project_summaries, 0)

   if 'totals' in reports:
      print("=======")
      print("Totals:")
      print("=======\n")
      print_table(*summarise_totals(projects, users, project_summaries, inv_total_time, bins))

   if 'projects' in reports:
      print("===========")
      print("Top projects:")
      print("===========\n")
      print_table(*summarise_projects(projects, users, project_summaries, inv_total_time, core_hours_adj, bins))

   if 'users' in reports:
      print("==========")
      print("Top users:")
      print("==========\n")
      print_simplestats(users, args.limitusers)
      print_table(*summarise_users(projects, users, inv_total_time, core_hours_adj, bins))

   if 'usersbyproject' in reports:
      print("===================")
      print("Top users by project:")
      print("===================\n")

      for project in sorted(projects):
         core_hours_adj = reduce((lambda x, k: x + projects[project][k]['core_hours_adj']), projects[project], 0)

         print("Project:", project)
         print_simplestats(projects[project], args.limitusers)
         print_table(*summarise_project(projects[project], inv_total_time, core_hours_adj, bins))


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


def percent(num):
   return "{0:.1%}".format(float(num))


# Take a range string of format [START][-[END]], where START and END are
# either integers, or dates of format YYYY[MM[DD[HH[MM[SS]]]]] in UTC.
#
# Return a tuple bounding the start and end of that range (start - inclusive,
# end - exclusive). Dates are returned as seconds since the epoch.
def parse_startend(range_str, type='date'):
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

   return start, end


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


# Run program (if we've not been imported)
# ---------------------------------------

if __name__ == "__main__":
   main()

