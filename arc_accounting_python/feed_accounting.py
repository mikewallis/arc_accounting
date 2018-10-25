#!/bin/env python

# Feed accounting records into database
# pip install --user mysql-connector

# Try and be python2 compatible
from __future__ import print_function

import argparse
import sys
import sge
import mysql.connector as mariadb
import syslog
import time
import yaml
import re


# Initialise data

fields = [
   'record',
   'qname',
   'hostname',
   'grp',
   'owner',
   'job_name',
   'job_number',
   'account',
   'priority',
   'submission_time',
   'start_time',
   'end_time',
   'failed',
   'exit_status',
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
   'project',
   'department',
   'granted_pe',
   'slots',
   'task_number',
   'cpu',
   'mem',
   'io',
   'category',
   'iow',
   'pe_taskid',
   'maxvmem',
   'arid',
   'ar_sub_time',
]

def main():
   # Command line arguments
   parser = argparse.ArgumentParser(description='Feed accounting data')
   parser.add_argument('--service', action='store', type=str, help="Service name to tag records")
   parser.add_argument('--accountingfile', action='store', type=str, help="Accounting file to read from")
   parser.add_argument('--syslogfile', action='store', type=str, help="Syslog file to read from")
   parser.add_argument('--sleep', action='store', type=int, default=300, help="Time to sleep between loop trips")
   parser.add_argument('--credfile', action='store', type=str, help="YAML credential file")
   args = parser.parse_args()

   if not args.service:
      raise SystemExit("Error: provide a service name argument")

   if args.credfile:
      with open(args.credfile, 'r') as stream:
         credentials = yaml.safe_load(stream)
   else:
      raise SystemExit("Error: provide a database credential file")

   sge_add_record = "INSERT INTO accounting_sge (service, " + \
      ", ".join([f for f in fields]) + \
      ") VALUES (%(service)s, " + \
      ", ".join(['%(' + f + ')s' for f in fields]) + \
      ")"

   syslog_select_record = "SELECT * FROM syslog_data WHERE cluster = %(cluster)s AND job = %(job)s"
   syslog_add_record = "INSERT INTO syslog_data (cluster, job) VALUES (%(cluster)s, %(job)s)"
   syslog_update_mpirun = "UPDATE syslog_data SET mpirun_file=%(mpirun_file)s WHERE cluster = %(cluster)s AND job = %(job)s"
   syslog_update_sgealloc = "UPDATE syslog_data SET alloc=%(alloc)s WHERE cluster = %(cluster)s AND job = %(job)s"
   syslog_update_sgenodes = "UPDATE syslog_data SET nodes_nodes=%(nodes_nodes)s, nodes_np=%(nodes_np)s, nodes_ppn=%(nodes_ppn)s, nodes_tpp=%(nodes_tpp)s WHERE cluster = %(cluster)s AND job = %(job)s"
   syslog_update_modules = "UPDATE syslog_data SET modules=%(modules)s WHERE cluster = %(cluster)s AND job = %(job)s"

   syslog.openlog()

   # Try connecting to database and processing records.
   # Retry after a delay if there's a failure.
   while True:
      try:
         # Connect to database
         db = mariadb.connect(**credentials)
         cursor = db.cursor(dictionary=True)

         # Initialise state

         if args.accountingfile:
            # Determine number of old sge records
            cursor.execute(
               "SELECT count(*) FROM accounting_sge WHERE service = %s",
               (args.service, ),
            )
            max_record = cursor.fetchall()[0][0]
            syslog.syslog("Found " + str(max_record) + " old " + \
                          args.service + " records")

            # Open input files and initialise state
            fh = open(args.accountingfile)
            record_num = 0

         if args.syslogfile:
            s_fh = open(args.syslogfile)

         # Process records as they come in
         while True:

            # SGE accounting records
            if args.accountingfile:
               insert = {}

               # - Process any waiting lines
               for record in sge.records(accounting=fh):
                  if record_num >= max_record:
                     if 'start' not in insert: insert['start'] = record_num
                     insert['end'] = record_num

                     record['service'] = args.service
                     record['record'] = record_num
                     record['grp'] = record['group']
                     cursor.execute(sge_add_record, record)

                  record_num += 1

               # - Commit bunch
               if 'start' in insert:
                  syslog.syslog("Inserting new " + args.service + \
                         " sge accounting records " + str(insert['start']) + \
                         " to " + str(insert['end']))
                  db.commit()


            # Syslog records
            if args.syslogfile:
               insert = {}

               # - Process any waiting lines
               for record in syslog_records(file=s_fh):

                  # Retrieve existing record
                  cursor.execute(syslog_select_record, record)
                  sql = cursor.fetchall()

                  # Create/retrieve if does not exist
                  if len(sql) < 1:
                     cursor.execute(syslog_add_record, record)
                     cursor.execute(syslog_select_record, record)
                     sql = cursor.fetchall()

                  # Update fields according to syslog data
                  if record['type'] == "mpirun":
                     # Add new mpirun file, comma separated (squash duplicates)
                     if sql[0].get('mpirun_file', None):
                        record['mpirun_file'] = ",".join(sorted(set([*sql[0]['mpirun_file'].split(','), record['mpirun_file']])))

                     # Update record (if we've changed it)
                     if sql[0]['mpirun_file'] != record['mpirun_file']:
#                        print("update mpirun file", record['job'], record['mpirun_file'])
                        cursor.execute(syslog_update_mpirun, record)
#                     else:
#                        print("SKIP mpirun")

                  elif record['type'] == "sgealloc":
                     if record['alloc']:
                        if sql[0]['alloc'] != record['alloc']:
#                           print("update sgealloc", sql, record)
                           cursor.execute(syslog_update_sgealloc, record)
#                        else:
#                           print("SKIP sgealloc")

                  elif record['type'] == "sgenodes":
                     if record['nodes_nodes']:
                        if sql[0]['nodes_nodes'] != int(record['nodes_nodes']) or \
                           sql[0]['nodes_np'] != int(record['nodes_np']) or \
                           sql[0]['nodes_ppn'] != int(record['nodes_ppn']) or \
                           sql[0]['nodes_tpp'] != int(record['nodes_tpp']):

#                           print("update sgenodes",sql,record)
                           cursor.execute(syslog_update_sgenodes, record)

                  elif record['type'] == "sgemodules" or \
                       record['type'] == "module load":
#                     print("module load", record)

                     if record['modules']:
                        m = record['modules'].split(':')

                        if sql[0]['modules']:
                           m.extend(sql[0]['modules'].split(','))

                        record['modules'] = ','.join(sorted(set(m)))

                        if sql[0]['modules'] != record['modules']:
#                           print("update modules", record)
                           cursor.execute(syslog_update_modules, record)

                  else:
                     print("What the?", record['type'])

#                  print("here")

                  db.commit()

#               # - Commit bunch
#               if 'start' in insert:
#                  syslog.syslog("Inserting new " + args.service + \
#                         " syslog records " + str(insert['start']) + \
#                         " to " + str(insert['end']))
#                  db.commit()

            print("sleeping...")
            time.sleep(args.sleep)
      except:
         syslog.syslog("Processing failed" + str(sys.exc_info()))

      time.sleep(args.sleep)

# Extract common features from syslog record
# Jul 16 14:35:52 login1 someone: <data>
syslog_def = re.compile(r"""
   (?P<month>\S+)\s+
   (?P<day>\d+)\s+
   (?P<time>[0-9:]+)\s+
   (?P<host>\S+)\s+
   (?P<user>\S+):\s+
   (?P<data>.*)
""", re.VERBOSE)

# mpirun data:
# mpirun cluster=arc3_prod job= file=/home/home01/someone/misc/prog/fortran/hellompi
mpirun_def = re.compile(r"""
   (?P<type>mpirun)\s+
   cluster=(?P<cluster>\S*)\s+
   job=(?P<job>\S*)\s+
   file=(?P<mpirun_file>\S*)
""", re.VERBOSE)

# sgealloc data:
# sgealloc cluster=arc3_prod job=2555412.1 24core-128G.q@dc1s0b1b=1,24core-128G.q@dc1s0b1d=1
sgealloc_def = re.compile(r"""
   (?P<type>sgealloc)\s+
   cluster=(?P<cluster>\S*)\s+
   job=(?P<job>\S*)\s+
   (?P<alloc>\S*)
""", re.VERBOSE)

# sgenodes data:
# sgenodes cluster=arc3_prod job=1222658.1 nodes=12 np=288 ppn=24 tpp=1
sgenodes_def = re.compile(r"""
   (?P<type>sgenodes)\s+
   cluster=(?P<cluster>\S*)\s+
   job=(?P<job>\S*)\s+
   nodes=(?P<nodes_nodes>\d*)\s+
   np=(?P<nodes_np>\d*)\s+
   ppn=(?P<nodes_ppn>\d*)\s+
   tpp=(?P<nodes_tpp>\d*)
""", re.VERBOSE)

# sgemodules data:
# sgemodules cluster= job=3609474.1624 licenses:sge:intel/17.0.1:openmpi/2.0.2:user
sgemodules_def = re.compile(r"""
   (?P<type>sgemodules)\s+
   cluster=(?P<cluster>\S*)\s+
   job=(?P<job>\S*)\s+
   (?P<modules>.*)
""", re.VERBOSE)

# module load data:
# job= module load sge intel/17.0.1 user openmpi/2.0.2 licenses (licenses:sge:intel/17.0.1:openmpi/2.0.2:user)

##DEBUG - entries don't have a cluster attribute
#   cluster=(?P<cluster>\S*)\s+

sgemoduleload_def = re.compile(r"""
   job=(?P<job>\S*)\s+
   (?P<type>module\s+load)\s+
   (?P<newmodules>.*)\s+
   \((?P<modules>[^)]*)\)
""", re.VERBOSE)

# Return syslog records we are interested in
def syslog_records(file):
   for line in file:
      r = syslog_def.match(line)
      if r:
         d = r.groupdict()

         # Process different types of record:

         for r_def in [ mpirun_def, sgemoduleload_def, sgealloc_def, sgenodes_def, sgemodules_def, sgemoduleload_def ]:
            r_match = r_def.match(r['data'])
            if r_match:
               d_match = r_match.groupdict()

               ##DEBUG - some records don't have a cluster yet
               d_match['cluster'] = 'arc3_prod'

               if d_match.get('cluster', False) and \
                  d_match.get('job', False):

                  yield({ **d, **d_match })
                  break


# Run program (if we've not been imported)
# ---------------------------------------

if __name__ == "__main__":
   main()
