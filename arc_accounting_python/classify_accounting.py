#!/bin/env python

# Classify database accounting records
# pip install --user mysql-connector

# Try and be python2 compatible
from __future__ import print_function

import argparse
import sys
import MySQLdb as mariadb
import syslog
import time
import yaml
import re
import os


def main():
   # Command line arguments
   parser = argparse.ArgumentParser(description='Classify accounting data')
   parser.add_argument('--service', action='store', type=str, help="Service name to tag records")
   parser.add_argument('--sleep', action='store', type=int, default=300, help="Time to sleep between loop trips")
   parser.add_argument('--credfile', action='store', type=str, help="YAML credential file")
   parser.add_argument('--debug', action='store_true', default=False, help="Print debugging messages")
   parser.add_argument('--limit', action='store', type=int, default=1000, help="Max number of records to classify at once")
   args = parser.parse_args()

   if not args.service:
      raise SystemExit("Error: provide a service name argument")

   if args.credfile:
      with open(args.credfile, 'r') as stream:
         credentials = yaml.safe_load(stream)
   else:
      raise SystemExit("Error: provide a database credential file")

   syslog.openlog()

   # Try connecting to database and processing records.
   # Retry after a delay if there's a failure.
   while True:
      if args.debug: print("Entering main loop")
      try:
         # Connect to database
         db = mariadb.connect(**credentials)
         cursor = db.cursor(mariadb.cursors.DictCursor)

         while True:
            while cursor.execute("SELECT * FROM jobs WHERE service = %s AND classified=FALSE LIMIT %s", (args.service, args.limit)):

               # Classify waiting records
               for sql in cursor: classify(db, sql)

               # Commit and obtain an up to date view of database state
               db.commit()

            if args.debug: print("sleeping...")
            time.sleep(args.sleep)

            # Update view of database state
            db.rollback()
      except:
         syslog.syslog("Processing failed" + str(sys.exc_info()))

      time.sleep(args.sleep)


mpirun_match = [
   { 'regex': '/vasp[/_0-9]', 'match': 'vasp' },
   { 'regex': '/relion[/_0-9]', 'match': 'relion' },
   { 'regex': '/lammps[/_0-9]', 'match': 'lammps' },
   { 'regex': '/(wrf|wrfmeteo|geogrid|metgrid).exe$', 'match': 'wrf' },
   { 'regex': '(^|/)amrvac$', 'match': 'amrvac' },
   { 'regex': '(^|/)cesm.exe$', 'match': 'cesm' },
   { 'regex': '(^|/)nek5000$', 'match': 'nek5000' },
   { 'regex': 'Had(ley|CM3L)[^/]*.exec', 'match': 'um' },
   { 'regex': '/(castep|CASTEP)([/-]|.mpi$|$)', 'match': 'castep' },
   { 'regex': '/OpenFOAM/', 'match': 'openfoam' },
   { 'regex': '/BISICLES/', 'match': 'bisicles' },
   { 'regex': '/gulp(.mpi)?$', 'match': 'gulp' },
   { 'regex': '/gmx_mpi$', 'match': 'gromacs' },
]

application_modules = [
   'abaqus',
   'amber',
   'ampl',
   'ansys',
   'ansysem',
   'ascp',
   'autodock',
   'bwa',
   'castep',
   'cdo',
   'cfdem',
   'comsol',
   'cp2k',
   'crystal17',
   'dakota',
   'delft3d',
   'dl_poly',
   'dosbox',
   'ehits',
   'fcm',
   'feff',
   'ferret',
   'flow3d',
   'gate',
   'gaussian',
   'geant4',
   'gmt',
   'gpaw',
   'gromacs',
   'h5utils',
   'idl',
   'lammps',
   'liggghts',
   'lpp',
   'matlab',
   'meep',
   'mesmer',
   'molpro',
   'mpas',
   'mpb',
   'mro',
   'namd',
   'nbo',
   'ncl',
   'nco',
   'ncview',
   'nwchem',
   'octave',
   'openeye',
   'openfoam',
   'orca',
   'paraview',
   'paraview-osmesa',
   'qiime',
   'relion',
   'rstudio',
   'samtools',
   'schrodinger',
   'starccm',
   'stata',
   'stir',
   'tetr',
   'visit',
   'vmd',

   'singularity',
   'R',
   'python',

   'grace', # plotting
   'gnuplot', # plotting
   'ploticus', # plotting
   'glpk', # library?
   'qhull', # library?
]

def classify(db, record):

   cursor = db.cursor()

   # Init classifications
   application = None
   appsource = None
   parallel = None


   # Check mpirun data

   if not application:
      cursor.execute(
         """
            SELECT
               mpiruns.name
            FROM
               job_to_mpirun, mpiruns
            WHERE
               job_to_mpirun.mpirunid = mpiruns.id
            AND
               job_to_mpirun.jobid = %s
         """,
         (record['id'], ),
      )

      for rec in cursor:
         file = rec[0]

         # Check if it's one of our applications
         if not application:
            r = re.search('^/apps[0-9]?/(infrastructure|applications|system|developers/[^/]+)/([^/]+)/([^/]+)/', file)
            if r:
               application = r.group(2)
               appsource = 'module'
               parallel = 'mpi'

         # Check if it's obvious from filename
         if not application:
            for m in mpirun_match:
               if re.search(m['regex'], file):
                  application = m['match']
                  appsource = 'user'
                  parallel = 'mpi'
                  break

         # Label with executable name instead
         if not application:
            #print(">>", file)
            application = "mpi:" + os.path.basename(file)
            appsource = 'user'
            parallel = 'mpi'


   # Check module data

   if not application:
      cursor.execute(
         """
            SELECT
               modules.name
            FROM
               job_to_module, modules
            WHERE
               job_to_module.moduleid = modules.id
            AND
               job_to_module.jobid = %s
         """,
         (record['id'], ),
      )

      for rec in cursor:
         module = rec[0]

         if not application:
            for app in application_modules:
               if re.search("^"+ app +"/", module):
                  application = app
                  appsource = 'module'
                  break


   # Check job details

   if not parallel:
      cursor.execute(
         """
            SELECT
               slots, granted_pe
            FROM
               sge
            WHERE
               service = %(service)s
            AND
               job = %(job)s
         """,
         record,
      )

      for acct in cursor:
         slots = acct[0]
         granted_pe = acct[1]
         if slots == 1:
            parallel = 'serial'
         elif granted_pe == "smp" or record['nodes_nodes'] == 1:
            parallel = 'shared'
         else:
            parallel = 'distrib'


   # Save results, mark as classified

   cursor.execute(
      """
         UPDATE
            jobs
         SET
            classified=TRUE,
            class_app = %s,
            class_appsource = %s,
            class_parallel = %s
         WHERE
            id = %s
      """,
      (application, appsource, parallel, record['id'], ),
   )


   #if args.debug: print(record['id'], "class_app", application)
   print(record['id'], application, appsource, parallel)


# Run program (if we've not been imported)
# ---------------------------------------

if __name__ == "__main__":
   main()
