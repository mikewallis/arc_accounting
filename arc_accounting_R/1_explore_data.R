#Explore data from accounting file
#Use Tidyverse and gdata


#Location of accounting files on ARC systems
#/services/sge_prod/default/common/accounting

#File is a plaintext file with 4 header rows
#Format is described : http://gridscheduler.sourceforge.net/htmlman/htmlman5/accounting.html?pathrev=V62u5_TAG

#To publish: replace uid with some hash, remove job name, environment

library(tidyverse)
library(gdata)
library(anytime)



#Read file into dataframe, here called accounting.log. Skip over first 4 lines (header)
column_names <- c("qname", "hostname", "group", "owner", "job_name", "job_number", "account", "priority", "submission_time",
                 "start_time", "end_time", "failed", "exit_status", "ru_wallclock", 
                 "ru_utime", "ru_stime", "ru_maxrss", "ru_ixrss", "ru_ismrss", "ru_idrss", "ru_isrss", "ru_minflt", 
                 "ru_majflt", "ru_nswap", "ru_inblock", "ru_oublock", "ru_msgsnd", "ru_msgrcv", "ru_nsignals",
                 "ru_nvcsw", "ru_nivcsw", "project", "department", "granted_pe", "slots", "task_number", "cpu",
                 "mem", "io", "category", "iow", "pe_taskid", "maxvmem", "arid", "ar_submission_time")

acc_log <- read_delim('accounting.log', skip = 4, delim = ':', col_names = FALSE)
colnames(acc_log) <- column_names

#Extract columns
acc_log2 <- data.frame(acc_log$job_number, acc_log$qname, acc_log$hostname, acc_log$owner, acc_log$project, acc_log$start_time, acc_log$end_time,
                       humanReadable(acc_log$maxvmem), acc_log$slots, acc_log$category)

#Set a start time and date for the accounting period
period_start = "2015-06-01 00:00:00 UTC"
period_end = "2015-06-30 23:59:59 UTC"

#convert to Unix time
period_start = as.numeric(as.POSIXct(period_start))
period_end = as.numeric(as.POSIXct(period_end))



