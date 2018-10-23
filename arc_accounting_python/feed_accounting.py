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

   syslog.openlog()

   # Try connecting to database and processing records.
   # Retry after a delay if there's a failure.
   while True:
      try:
         # Connect to database
         db = mariadb.connect(**credentials)
         cursor = db.cursor()

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
                         " records " + str(insert['start']) + " to " + \
                         str(insert['end']))
                  db.commit()

            time.sleep(args.sleep)
      except:
         syslog.syslog("Processing failed" + str(sys.exc_info()))

      time.sleep(args.sleep)

# Run the main program
main()
