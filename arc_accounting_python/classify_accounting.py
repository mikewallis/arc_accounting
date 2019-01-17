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
import sge


def main():
   # Command line arguments
   parser = argparse.ArgumentParser(description='Classify accounting data')
   parser.add_argument('--services', action='store', type=str, help="Service names to process records for")
   parser.add_argument('--sleep', action='store', type=int, default=300, help="Time to sleep between loop trips")
   parser.add_argument('--credfile', action='store', type=str, help="YAML credential file")
   parser.add_argument('--debug', action='store_true', default=False, help="Print debugging messages")
   parser.add_argument('--limit', action='store', type=int, default=1000, help="Max number of records to classify at once")
   parser.add_argument('--reportmpi', action='store_true', default=False, help="Report on mpirun exes we don't have regexes for")
   args = parser.parse_args()

   if args.credfile:
      with open(args.credfile, 'r') as stream:
         credentials = yaml.safe_load(stream)
   else:
      raise SystemExit("Error: provide a database credential file")

   if args.reportmpi:
      reportmpi(credentials)
      raise SystemExit

   if not args.services:
      raise SystemExit("Error: provide service name arguments")

   args.services = commasep_list(args.services)

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
            for service in args.services:

               # Get service id
               sql = sge.sql_get_create(
                  cursor,
                  "SELECT id,name FROM services WHERE name = %s",
                  (service,),
                  insert="INSERT INTO services (name) VALUES (%s)",
                  first=True,
               )
               serviceid = sql['id']
               db.commit()

               # Search for unclassified records
               while cursor.execute("SELECT * FROM jobs WHERE serviceid = %s AND classified=FALSE LIMIT %s", (serviceid, args.limit)):

                  # Classify waiting records
                  for sql in cursor: classify(db, sql, service, args.debug)

                  # Commit and obtain an up to date view of database state
                  db.commit()

            if args.debug: print("sleeping...")
            time.sleep(args.sleep)

            # Update view of database state
            db.rollback()
      except:
         syslog.syslog("Processing failed" + str(sys.exc_info()))

      time.sleep(args.sleep)


def reportmpi(credentials):
   # Connect to database
   db = mariadb.connect(**credentials)
   cursor = db.cursor()

   cursor.execute("SELECT name FROM mpiruns")
   for record in cursor:
      file = record[0]
      (application, appsource, parallel) = classify_mpirun(file)
      print(file, "=>", application)

mpirun_match = [
   { 'regex': '/vasp[/_0-9]', 'match': 'vasp', 'domain': 'materials' },
   { 'regex': '/relion[/_0-9]', 'match': 'relion', 'domain': 'cryoem' },
   { 'regex': '/lammps[/_0-9]', 'match': 'lammps', 'domain': 'materials' },
   { 'regex': '/(wrf|wrfmeteo|geogrid|metgrid).exe$', 'match': 'wrf', 'domain': 'climate_ocean' },
   { 'regex': '[_/]wrf[_/0-9-].*/real.exe$', 'match': 'wrf', 'domain': 'climate_ocean' },
   { 'regex': '(^|/)amrvac$', 'match': 'amrvac', 'domain': 'fluids' },
   { 'regex': '((^|/)cesm.exe$|/cesm[/0-9])', 'match': 'cesm', 'domain': 'climate_ocean' },
   { 'regex': '(^|/)nek5000$', 'match': 'nek5000', 'domain': 'fluids' },
   { 'regex': 'Had(ley|CM3L)[^/]*.exec', 'match': 'um', 'domain': 'climate_ocean' },
   { 'regex': '/(castep)([/-]|.mpi$|$)', 'match': 'castep', 'domain': 'materials' },
   { 'regex': '/OpenFOAM/', 'match': 'openfoam', 'domain': 'fluids' },
   { 'regex': '/BISICLES/', 'match': 'bisicles', 'domain': 'climate_ocean' },
   { 'regex': '/gulp(.mpi)?$', 'match': 'gulp', 'domain': 'materials' },
   { 'regex': '/gmx_mpi$', 'match': 'gromacs', 'domain': 'molecular_dynam' },
   { 'regex': '/dedalus[_/-]', 'match': 'dedalus', 'domain': 'fluids' },
   { 'regex': '/python[0-9.]*?$', 'match': 'python' }, # Last: very generic classification!
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

def classify_mpirun(file):
   application = None
   appsource = None
   parallel = None

   # Check if it's one of our applications
   r = re.search('^/apps[0-9]?/(infrastructure|applications|system|developers/[^/]+)/([^/]+)/([^/]+)/', file)
   if r:
      application = r.group(2)
      appsource = 'module'
      parallel = 'mpi'

   # Check if it's obvious from filename
   if not application:
      for m in mpirun_match:
         if re.search(m['regex'], file, re.IGNORECASE):
            application = m['match']
            appsource = 'user'
            parallel = 'mpi'
            break

   return (application, appsource, parallel)


def classify(db, record, service, debug):

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

         # Attempt to label application based on mpirun
         (application, appsource, parallel) = classify_mpirun(file)

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
               serviceid = %(serviceid)s
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


   if debug: print(service, record['job'], application, appsource, parallel)


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
