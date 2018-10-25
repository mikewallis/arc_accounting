Yes, this is yet another set of accounting scripts in yet another
language. Sorry.

sge.py is a reusable library of gridengine-related routines.  accounting
reports on gridengine accounting data

Requires python 3.5 or higher.

You may need to install following packages:

   pip install --user tabulate
   pip install --user python-dateutil
   pip install --user pytz

Example usage to report on combined arc3 and arc2 usage (copy each
service's accounting file to an appropriate location first):

   ./accounting \
      --dates 201801-201805 \
      --coreprojects --limitusers 10 \
      --accountingfile=/tmp/issmcd/accounting_arc3 \
      --accountingfile=/tmp/issmcd/accounting_arc2

Added '--bymonth' or '--byyear' to see how usage varies within the date
range.

See "./accounting --help" for more details. All arguments are optional.

Alternatively, program can be imported into an interactive python session:

    import accounting as ac
    print(ac.args)
    ac.main()
