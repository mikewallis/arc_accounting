Yes, this is yet another set of accounting scripts in yet another
language. Sorry.

sge.py is a reusable library of gridengine-related routines.  accounting
reports on gridengine accounting data

Mostly tested under python3, but should work under python2.

You may need to install package 'tabulate':

   pip install --user tabulate

Example usage to report on combined arc3 and arc2 usage (copy each
service's accounting file to an appropriate location first):

   ./accounting \
      --date 20170101-20180101 \
      --coreowners --limitusers 10 \
      --accountingfile=/tmp/issmcd/accounting_arc3 \
      --accountingfile=/tmp/issmcd/accounting_arc2

See "./accounting --help" for more details. All arguments are optional.
