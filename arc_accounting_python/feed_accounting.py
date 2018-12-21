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
import os


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
   parser.add_argument('--sawrapdir', action='store', type=str, help="qstat3 sawrap dir to read node availability data from")
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

   sge_add_record = "INSERT INTO sge (" + \
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

         # Get service id
         sql = sge.sql_get_create(
            cursor,
            "SELECT id FROM services WHERE name = %s",
            (args.service,),
            insert="INSERT INTO services (name) VALUES (%s)",
            first=True,
         )
         serviceid = sql['id']
         db.commit()

         # Initialise state

         if args.accountingfile:
            # Determine number of old sge records
            cursor.execute(
               "SELECT count(*) FROM sge WHERE service = %s",
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
                        "SELECT * FROM jobs WHERE service = %(service)s AND job = %(job)s",
                        {
                           'service': args.service,
                           'job': record['job'],
                           'classified': False,
                        },
                        insert="INSERT INTO jobs (service, job, classified) VALUES (%(service)s, %(job)s, %(classified)s)",
                        update="UPDATE jobs SET classified=%(classified)s WHERE service = %(service)s AND job = %(job)s",
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
                     "SELECT * FROM jobs WHERE service = %(service)s AND job = %(job)s",
                     record,
                     insert="INSERT INTO jobs (service, job, classified) VALUES (%(service)s, %(job)s, %(classified)s)",
                     first=True,
                  )

                  # Update fields according to syslog data
                  if record['type'] == "mpirun":

                     # Get mpirun file record
                     mpirun = sge.sql_get_create(
                        cursor,
                        "SELECT id, name FROM mpiruns WHERE name = %(name)s",
                        {
                           'name': record['mpirun_file'],
                        },
                        insert="INSERT INTO mpiruns (name, name_sha1) VALUES (%(name)s, SHA1(%(name)s))",
                        first=True,
                     )

                     # Add mpirun file to job record if needed
                     # Mark job as needing fresh classification
                     sge.sql_get_create(
                        cursor,
                        "SELECT * FROM job_to_mpirun WHERE jobid = %(jobid)s AND mpirunid = %(mpirunid)s",
                        {
                           'jobid': sql['id'],
                           'mpirunid': mpirun['id'],
                        },
                        insert="INSERT INTO job_to_mpirun (jobid, mpirunid) VALUES (%(jobid)s, %(mpirunid)s)",
                        oninsert="UPDATE jobs SET classified=FALSE WHERE id = %(jobid)s",
                     )

                     if args.debug: print(record['job'], "mpirun", record['mpirun_file'])

                  elif record['type'] == "sgealloc":
                     if record['alloc']:
                        hosts = sql['hosts']

                        for alloc in record['alloc'].split(','):
                           r = re.match(r"([^@]+)@([^=]+)=(\d+)", alloc)
                           if r:
                              q = r.group(1)
                              h = r.group(2)
                              slots = r.group(3)
                              hosts += 1

                              # Get queue record
                              rec_q = sql_insert_queue(cursor, q)

                              # Get host record
                              rec_h = sql_insert_host(cursor, h)

                              # Add allocation to job record if needed
                              # Mark job as needing fresh classification
                              sge.sql_get_create(
                                 cursor,
                                 "SELECT * FROM job_to_alloc WHERE jobid = %(jobid)s AND hostid = %(hostid)s AND queueid = %(queueid)s",
                                 {
                                    'jobid': sql['id'],
                                    'hostid': rec_h['id'],
                                    'queueid': rec_q['id'],
                                    'slots': slots,
                                    'hosts': hosts,
                                 },
                                 insert="INSERT INTO job_to_alloc (jobid, hostid, queueid, slots) VALUES (%(jobid)s, %(hostid)s, %(queueid)s, %(slots)s)",
                                 oninsert="UPDATE jobs SET classified=FALSE, hosts=%(hosts)s WHERE id = %(jobid)s",
                              )

                              if args.debug: print(record['job'], "update sgealloc")

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
                              "SELECT id, name FROM modules WHERE name = %(name)s",
                              {
                                 'name': module,
                              },
                              insert="INSERT INTO modules (name, name_sha1) VALUES (%(name)s, SHA1(%(name)s))",
                              first=True,
                           )

                           # Add module file to job record if needed
                           # Mark job as needing fresh classification
                           sge.sql_get_create(
                              cursor,
                              "SELECT * FROM job_to_module WHERE jobid = %(jobid)s AND moduleid = %(moduleid)s",
                              {
                                 'jobid': sql['id'],
                                 'moduleid': mod['id'],
                              },
                              insert="INSERT INTO job_to_module (jobid, moduleid) VALUES (%(jobid)s, %(moduleid)s)",
                              oninsert="UPDATE jobs SET classified=FALSE WHERE id = %(jobid)s",
                           )

                        if args.debug: print(record['job'], "module", record['modules'])

                  elif record['type'] == "sge-allocator: Resource stats nvidia":

                     # Get host record
                     rec_h = sql_insert_host(cursor, record['host'])

                     # Get coproc record
                     # (tag with hostname as coproc name is currently just a
                     # index on a host. Not necessary if we started using the
                     # card UUID instead)
                     rec_cp = sge.sql_get_create(
                        cursor,
                        "SELECT id, name, model FROM coprocs WHERE name = %(name)s",
                        {
                           'name': record['host'] +":"+ record['name'],
                           'model': record['model'],
                           'memory': 1024*1024*int(record['coproc_max_mem']), # bytes
                        },
                        insert="INSERT INTO coprocs (name, name_sha1, model, model_sha1, memory) VALUES (%(name)s, SHA1(%(name)s), %(model)s, SHA1(%(model)s), %(memory)s)",
                        first=True,
                     )

                     # Add to job record (and update coproc stats) if not seen this allocation before
                     sge.sql_get_create(
                        cursor,
                        "SELECT jobid FROM job_to_coproc WHERE jobid = %(jobid)s AND hostid = %(hostid)s AND coprocid = %(coprocid)s",
                        {
                           'jobid': sql['id'],
                           'hostid': rec_h['id'],
                           'coprocid': rec_cp['id'],
                           'coproc': sql['coproc'] +1,
                           'coproc_max_mem': 1024*1024*int(record['coproc_max_mem']), # bytes
                           'coproc_cpu': float(record['coproc_cpu'])/100, # s
                           'coproc_mem': float(record['coproc_mem'])/(100*1024), # Gib * s
                           'coproc_maxvmem': 1024*1024*int(record['coproc_maxvmem']), # bytes
                           'sum_coproc_max_mem': sum([1024*1024*int(record['coproc_max_mem']), sql['coproc_max_mem']]), # bytes
                           'sum_coproc_cpu': sum([float(record['coproc_cpu'])/100, sql['coproc_cpu']]), # s
                           'sum_coproc_mem': sum([float(record['coproc_mem'])/(100*1024), sql['coproc_mem']]), # Gib * s
                           'sum_coproc_maxvmem': sum([1024*1024*int(record['coproc_maxvmem']), sql['coproc_maxvmem']]), # bytes
                        },
                        insert="INSERT INTO job_to_coproc (jobid, hostid, coprocid, coproc_max_mem, coproc_cpu, coproc_mem, coproc_maxvmem) VALUES (%(jobid)s, %(hostid)s, %(coprocid)s, %(coproc_max_mem)s, %(coproc_cpu)s, %(coproc_mem)s, %(coproc_maxvmem)s)",
                        oninsert="UPDATE jobs SET classified=FALSE, coproc=%(coproc)s, coproc_max_mem=%(sum_coproc_max_mem)s, coproc_cpu=%(sum_coproc_cpu)s, coproc_mem=%(sum_coproc_mem)s, coproc_maxvmem=%(sum_coproc_maxvmem)s WHERE id = %(jobid)s",
                     )

                     if args.debug: print(record['job'], "update gpu stats")

                  elif record['type'] == "sgeepilog":
                     if record['epilog_copy']:
                        if sql['epilog_copy'] != int(record['epilog_copy']):

                           if args.debug: print(record['job'], "update sgeepilog")
                           sql_update_job(cursor, "epilog_copy=%(epilog_copy)s", record)

                  else:
                     if args.debug: print("What the?", record['type'])

                  db.commit()

            # Node availability data
            if args.sawrapdir:
               process_sawrapdir(args, db, cursor, serviceid)

            if args.debug: print("sleeping...")
            time.sleep(args.sleep)
      except:
         syslog.syslog("Processing failed" + str(sys.exc_info()))

      time.sleep(args.sleep)


def process_sawrapdir(args, db, cursor, serviceid):
   # Check we have all historical data
   for fname in os.listdir(args.sawrapdir):
      qstat3 = os.path.join(args.sawrapdir, fname)

      # Retrieve progress
      sql = sge.sql_get_create(
         cursor,
         "SELECT active,state FROM data_source_state WHERE service = %s AND host = %s AND name = %s",
         (args.service, socket.getfqdn(), qstat3 ),
         insert="INSERT INTO data_source_state (service, host, name) VALUES (%s, %s, %s)",
         first=True,
      )

      # Skip if file no longer active
      if not sql['active']: continue

      if args.debug: print("Processing", qstat3)
      line_num = 0

      for line in sge.open_file(qstat3):
         line_num += 1
         if line_num <= sql['state']: continue

         r = re.match(r"""
               (?P<time>\d+)\s+
               (?P<queue>\S+)@
               (?P<host>\S+?)\.\S+\s+
               [BIPC]+\s+
               (?P<slots_reserved>\d+)/
               (?P<slots_used>\d+)/
               (?P<slots_total>\d+)\s+
               \S+\s+
               \S+\s+
               (?P<flags>\S+)?
            """,
            line,
            re.VERBOSE,
         )
         if r:
            d = r.groupdict()

            # Lookup relationships
            rec_q = sql_insert_queue(cursor, d['queue'])
            rec_h = sql_insert_host(cursor, d['host'])

            # Fill out status
            d['serviceid'] = serviceid
            d['queueid'] = rec_q['id']
            d['hostid'] = rec_h['id']
            d['ttl'] = 10*60 # 10 minutes by default
            d['enabled'] = True
            d['available'] = True
            if d['flags']:
               d['enabled'] = "d" not in d['flags']
               if re.match(r"[cdsuE]", d['flags']): d['available'] = False

            # Insert record if not already there
            sge.sql_get_create(
               cursor,
               "SELECT * FROM availability WHERE serviceid = %(serviceid)s AND time = %(time)s AND hostid = %(hostid)s AND queueid = %(queueid)s",
               d,
               insert="INSERT INTO availability (serviceid, time, hostid, queueid, slots_reserved, slots_used, slots_total, enabled, available, ttl) VALUES (%(serviceid)s, %(time)s, %(hostid)s, %(queueid)s, %(slots_reserved)s, %(slots_used)s, %(slots_total)s, %(enabled)s, %(available)s, %(ttl)s)",
            )
            db.commit()

      # If file is older than 3 days, mark as inactive
      # (to avoid reprocessing stuff all the time)
      st = os.stat(qstat3)
      active = True
      if time.time() - max([st.st_mtime, st.st_ctime]) > 3*24*3600: active = False

      # Record progress (lazy - do at end of file)
      cursor.execute(
         "UPDATE data_source_state SET active=%s,state=%s WHERE service = %s AND host = %s AND name = %s",
         (active, line_num, args.service, socket.getfqdn(), qstat3),
      )

      db.commit()


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
   model=(?P<model>\S+)\s+.*
   max_mem=(?P<coproc_max_mem>\S+)\s+.*
   sm=(?P<coproc_cpu>\S+)\s+.*
   fb=(?P<coproc_mem>\S+)\s+.*
   maxfb=(?P<coproc_maxvmem>\S+)
""", re.VERBOSE)

# epilog copy data:
#sgeepilog cluster=arc3_prod job=12121.1 copy disk_out 0 seconds

sgeepilog_def = re.compile(r"""
   (?P<type>sgeepilog).*
   cluster=(?P<cluster>\S*)\s+
   job=(?P<job>\S+)\s+
   copy\s+disk_out\s+(?P<epilog_copy>\S+)\s+seconds
""", re.VERBOSE)

# Return syslog records we are interested in
def syslog_records(file):
   for line in file:
      r = syslog_def.match(line)
      if r:
         d = r.groupdict()

         # Process different types of record:

         for r_def in [ mpirun_def, sgemoduleload_def, sgealloc_def, sgenodes_def, sgemodules_def, sgegpustats_def, sgeepilog_def ]:
            r_match = r_def.match(r['data'])

            if r_match:
               d_match = r_match.groupdict()

               if d_match.get('job', False):
                  yield({ **d, **d_match })
                  break


def sql_update_job(cursor, update, data):
   cursor.execute(
      "UPDATE jobs SET classified=%(classified)s, " + update + " WHERE service = %(service)s AND job = %(job)s",
      data,
   )


def sql_insert_queue(cursor, queue):
   return(sge.sql_get_create(
      cursor,
      "SELECT id, name FROM queues WHERE name = %(name)s",
      {
         'name': queue,
      },
      insert="INSERT INTO queues (name, name_sha1) VALUES (%(name)s, SHA1(%(name)s))",
      first=True,
   ))


def sql_insert_host(cursor, host):
   return(sge.sql_get_create(
      cursor,
      "SELECT id, name FROM hosts WHERE name = %(name)s",
      {
         'name': host,
      },
      insert="INSERT INTO hosts (name, name_sha1) VALUES (%(name)s, SHA1(%(name)s))",
      first=True,
   ))


# Run program (if we've not been imported)
# ---------------------------------------

if __name__ == "__main__":
   main()
