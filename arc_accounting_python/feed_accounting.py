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
   parser.add_argument('--credfile', action='store', type=str, help="JSON credential file")
   args = parser.parse_args()

   if not args.service:
      raise SystemExit("Error: provide a service name argument")

   add_record = "INSERT INTO accounting_sge (service, " + \
      ", ".join([f for f in fields]) + \
      ") VALUES (%(service)s, " + \
      ", ".join(['%(' + f + ')s' for f in fields]) + \
      ")"

   syslog.openlog()

   while True:
      try:
         syslog.syslog("Inserting new accounting data")

         # Connect to database
         db = mariadb.connect(database='test_accounting')
         cursor = db.cursor()

         # Determine number of old records
         cursor.execute("SELECT count(*) FROM accounting_sge WHERE service = %s", (args.service, ))
         max_record = cursor.fetchall()[0][0]
         syslog.syslog("Found " + str(max_record) + " old " + args.service + " records")

         # Insert new records
         insert = {}
         for (i, record) in enumerate(sge.records(accounting=args.accountingfile)):
            if i >= max_record:
               if 'start' not in insert: insert['start'] = i
               insert['end'] = i

               record['service'] = args.service
               record['record'] = i
               record['grp'] = record['group']
               cursor.execute(add_record, record)

         syslog.syslog("Inserting new " + args.service + " records " + str(insert['start']) + " to " + str(insert['end']))

         # Commit
         db.commit()
         cursor.close()
         db.close()
      except:
         syslog.syslog("Database insert failed" + str(sys.exc_info()))

      time.sleep(args.sleep)

# Run the main program
main()
