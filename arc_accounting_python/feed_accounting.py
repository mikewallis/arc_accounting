#!/bin/env python

# Feed accounting records into database
# pip install --user mysql-connector

# Try and be python2 compatible
from __future__ import print_function

import argparse
import sys
import sge
import MySQLdb as mariadb
import syslog
import time
import yaml
import re
import socket


# Initialise data

fields = [
   'service',
   'record',
   'job',

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
   parser.add_argument('--debug', action='store_true', default=False, help="Print debugging messages")
   args = parser.parse_args()

   if not args.service:
      raise SystemExit("Error: provide a service name argument")

   if args.credfile:
      with open(args.credfile, 'r') as stream:
         credentials = yaml.safe_load(stream)
   else:
      raise SystemExit("Error: provide a database credential file")

   sge_add_record = "INSERT INTO accounting_sge (" + \
      ", ".join([f for f in fields]) + \
      ") VALUES (" + \
      ", ".join(['%(' + f + ')s' for f in fields]) + \
      ")"

   syslog.openlog()

   # Try connecting to database and processing records.
   # Retry after a delay if there's a failure.
   while True:
      if args.debug: print("Entering main loop")
      try:
         # Connect to database
         db = mariadb.connect(**credentials)
         cursor = db.cursor(mariadb.cursors.DictCursor)

         # Initialise state

         if args.accountingfile:
            # Determine number of old sge records
            cursor.execute(
               "SELECT count(*) FROM accounting_sge WHERE service = %s",
               (args.service, ),
            )
            acc_max_record = cursor.fetchall()[0]['count(*)']
            syslog.syslog("Found " + str(acc_max_record) + " old sge " + \
                          args.service + " records")

            # Open input files and initialise state
            fh = open(args.accountingfile)
            acc_record_num = 0

         if args.syslogfile:
            # Determine number of old syslog records

            sql = sge.sql_get_create(
               cursor,
               "SELECT * FROM data_source_state WHERE service = %s AND host = %s AND name = %s",
               (args.service, socket.getfqdn(), args.syslogfile ),
               insert="INSERT INTO data_source_state (service, host, name) VALUES (%s, %s, %s)",
               first=True,
            )

            sys_max_record = sql['state']
            syslog.syslog("Found " + str(sys_max_record) + " old syslog " + \
                          args.service + " records")

            s_fh = open(args.syslogfile)
            sys_record_num = 0


         # Process records as they come in
         while True:

            # SGE accounting records
            if args.accountingfile:

               # - Process any waiting lines
               for record in sge.records(accounting=fh):
                  if acc_record_num >= acc_max_record:

                     record['service'] = args.service
                     record['record'] = acc_record_num
                     record['job'] = str(record['job_number']) + "." + str(record['task_number'] or 1)

                     if args.debug: print(record['job'], "record accounting")

                     cursor.execute(sge_add_record, record)

                     # Record job as requiring classification
                     sge.sql_get_create(
                        cursor,
                        "SELECT * FROM job_data WHERE service = %(service)s AND job = %(job)s",
                        {
                           'service': args.service,
                           'job': record['job'],
                           'classified': False,
                        },
                        insert="INSERT INTO job_data (service, job, classified) VALUES (%(service)s, %(job)s, %(classified)s)",
                        update="UPDATE job_data SET classified=%(classified)s WHERE service = %(service)s AND job = %(job)s",
                        first=True,
                     )

                     db.commit()

                  acc_record_num += 1

            # Syslog records
            if args.syslogfile:

               # - Process any waiting lines
               for record in syslog_records(file=s_fh):
                  sys_record_num += 1

                  # Skip processed lines
                  if sys_record_num < sys_max_record:
                     if args.debug: print("skipping line", args.syslogfile, sys_record_num)
                     continue

                  # Record line as processed
                  cursor.execute(
                     "UPDATE data_source_state SET state=%s WHERE service = %s AND host = %s AND name = %s",
                     (sys_record_num, args.service, socket.getfqdn(), args.syslogfile ),
                  )

                  # Allocate to service, flag as needing classification if
                  # we update the record
                  record['service'] = args.service
                  record['classified'] = False

                  # Retrieve/create existing record
                  sql = sge.sql_get_create(
                     cursor,
                     "SELECT * FROM job_data WHERE service = %(service)s AND job = %(job)s",
                     record,
                     insert="INSERT INTO job_data (service, job, classified) VALUES (%(service)s, %(job)s, %(classified)s)",
                     first=True,
                  )

                  # Update fields according to syslog data
                  if record['type'] == "mpirun":

                     # Get mpirun file record
                     mpirun = sge.sql_get_create(
                        cursor,
                        "SELECT id, name FROM mpirun WHERE name = %(name)s",
                        { 'name': record['mpirun_file'] },
                        insert="INSERT INTO mpirun (name, name_sha1) VALUES (%(name)s, SHA1(%(name)s))",
                        first=True,
                     )

                     # Add mpirun file to job record if needed
                     # Mark job as needing fresh classification
                     sge.sql_get_create(
                        cursor,
                        "SELECT * FROM job_to_mpirun WHERE jobid = %(jobid)s AND mpirunid = %(mpirunid)s",
                        { 'jobid': sql['id'], 'mpirunid': mpirun['id'] },
                        insert="INSERT INTO job_to_mpirun (jobid, mpirunid) VALUES (%(jobid)s, %(mpirunid)s)",
                        oninsert="UPDATE job_data SET classified=FALSE WHERE id = %(jobid)s",
                        first=True,
                     )

                     if args.debug: print(record['job'], "mpirun", record['mpirun_file'])

                  elif record['type'] == "sgealloc":
                     if record['alloc']:
                        if sql['alloc'] != record['alloc']:
                           if args.debug: print(record['job'], "update sgealloc")
                           sql_update_job(cursor, "alloc=%(alloc)s", record)

                  elif record['type'] == "sgenodes":
                     if record['nodes_nodes']:
                        if sql['nodes_nodes'] != int(record['nodes_nodes']) or \
                           sql['nodes_np'] != int(record['nodes_np']) or \
                           sql['nodes_ppn'] != int(record['nodes_ppn']) or \
                           sql['nodes_tpp'] != int(record['nodes_tpp']):

                           if args.debug: print(record['job'], "update sgenodes")
                           sql_update_job(cursor, "nodes_nodes=%(nodes_nodes)s, nodes_np=%(nodes_np)s, nodes_ppn=%(nodes_ppn)s, nodes_tpp=%(nodes_tpp)s", record)

                  elif record['type'] == "sgemodules" or \
                       record['type'] == "module load":

                     if record['modules']:
                        for module in record['modules'].split(':'):
                           # Get module record
                           mod = sge.sql_get_create(
                              cursor,
                              "SELECT id, name FROM module WHERE name = %(name)s",
                              { 'name': module },
                              insert="INSERT INTO module (name, name_sha1) VALUES (%(name)s, SHA1(%(name)s))",
                              first=True,
                           )

                           # Add module file to job record if needed
                           # Mark job as needing fresh classification
                           sge.sql_get_create(
                              cursor,
                              "SELECT * FROM job_to_module WHERE jobid = %(jobid)s AND moduleid = %(moduleid)s",
                              { 'jobid': sql['id'], 'moduleid': mod['id'] },
                              insert="INSERT INTO job_to_module (jobid, moduleid) VALUES (%(jobid)s, %(moduleid)s)",
                              oninsert="UPDATE job_data SET classified=FALSE WHERE id = %(jobid)s",
                              first=True,
                           )

                        if args.debug: print(record['job'], "module", record['modules'])

                  elif record['type'] == "sge-allocator: Resource stats nvidia":

                     record['coproc_names'] = record['host'] + ":" + record['name']

                     if sql.get('coproc_names', None):
                        record['coproc_names'] = "/".join(sorted(set([*sql['coproc_names'].split('/'), record['coproc_names']])))

                     # Skip if this is a record we've seen before
                     if record['coproc_names'] == sql.get('coproc_names', None): continue

                     # Convert gpu stats to coproc stats and add to record
                     record['coproc_max_mem'] = sum([1024*1024*int(record['coproc_max_mem']), sql['coproc_max_mem']]) # bytes
                     record['coproc_cpu'] = sum([float(record['coproc_cpu'])/100, sql['coproc_cpu']]) # s
                     record['coproc_mem'] = sum([float(record['coproc_mem'])/(100*1024), sql['coproc_mem']]) # Gib * s
                     record['coproc_maxvmem'] = sum([1024*1024*int(record['coproc_maxvmem']), sql['coproc_maxvmem']]) # bytes

                     if args.debug: print(record['job'], "update gpu stats")
                     sql_update_job(cursor, "coproc_names=%(coproc_names)s, coproc_max_mem=%(coproc_max_mem)s, coproc_cpu=%(coproc_cpu)s, coproc_mem=%(coproc_mem)s, coproc_maxvmem=%(coproc_maxvmem)s", record)

                  else:
                     if args.debug: print("What the?", record['type'])

                  db.commit()

            if args.debug: print("sleeping...")
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
   job=(?P<job>\S+)\s+
   file=(?P<mpirun_file>\S+)
""", re.VERBOSE)

# sgealloc data:
# sgealloc cluster=arc3_prod job=2555412.1 24core-128G.q@dc1s0b1b=1,24core-128G.q@dc1s0b1d=1
sgealloc_def = re.compile(r"""
   (?P<type>sgealloc)\s+
   cluster=(?P<cluster>\S*)\s+
   job=(?P<job>\S+)\s+
   (?P<alloc>\S+)
""", re.VERBOSE)

# sgenodes data:
# sgenodes cluster=arc3_prod job=1222658.1 nodes=12 np=288 ppn=24 tpp=1
sgenodes_def = re.compile(r"""
   (?P<type>sgenodes)\s+
   cluster=(?P<cluster>\S*)\s+
   job=(?P<job>\S+)\s+
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
   job=(?P<job>\S+)\s+
   (?P<modules>.*)
""", re.VERBOSE)

# module load data:
# job= module load sge intel/17.0.1 user openmpi/2.0.2 licenses (licenses:sge:intel/17.0.1:openmpi/2.0.2:user)

##DEBUG - entries don't have a cluster attribute
#   cluster=(?P<cluster>\S*)\s+

sgemoduleload_def = re.compile(r"""
   job=(?P<job>\S+)\s+
   (?P<type>module\s+load)\s+
   (?P<newmodules>.*)\s+
   \((?P<modules>[^)]*)\)
""", re.VERBOSE)

# sge-allocator gpu stats data:
# sge-allocator: Resource stats nvidia pid=81962 job=700000.1 secs=1684 name=3 model=coproc_p100 poll=10 dev=1 max_mem=12193 samples=167 sm=0 mem=0 enc=0 dec=0 fb=0 maxfb=0 bar1=3340 maxbar1=2

sgegpustats_def = re.compile(r"""
   (?P<type>sge-allocator:\s+Resource\s+stats\s+nvidia).*
   job=(?P<job>\S+)\s+.*
   secs=(?P<secs>\S+)\s+.*
   name=(?P<name>\S+)\s+.*
   max_mem=(?P<coproc_max_mem>\S+)\s+.*
   sm=(?P<coproc_cpu>\S+)\s+.*
   fb=(?P<coproc_mem>\S+)\s+.*
   maxfb=(?P<coproc_maxvmem>\S+)
""", re.VERBOSE)

# Return syslog records we are interested in
def syslog_records(file):
   for line in file:
      r = syslog_def.match(line)
      if r:
         d = r.groupdict()

         # Process different types of record:

         for r_def in [ mpirun_def, sgemoduleload_def, sgealloc_def, sgenodes_def, sgemodules_def, sgegpustats_def ]:
            r_match = r_def.match(r['data'])

            if r_match:
               d_match = r_match.groupdict()

               if d_match.get('job', False):
                  yield({ **d, **d_match })
                  break


def sql_update_job(cursor, update, data):
   cursor.execute(
      "UPDATE job_data SET classified=%(classified)s, " + update + " WHERE service = %(service)s AND job = %(job)s",
      data,
   )

# Run program (if we've not been imported)
# ---------------------------------------

if __name__ == "__main__":
   main()
