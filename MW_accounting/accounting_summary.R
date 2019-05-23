# this looks awful. MW.
options(scipen=999)

# load some data - filename is "accounting" because that's what it's called
myData <- read.csv("accounting",sep=":", stringsAsFactors = FALSE, header = FALSE)
colnames(myData) <- c('qname','hostname','group','owner','job_name','job_ID','account','priority','submission_time','start_time','end_time','failed','exit_status','ru_wallclock','ru_utime','ru_stime','ru_maxrss','ru_ixrss','ru_ismrss','ru_idrss','ru_isrss','ru_minflt','ru_majflt','ru_nswap','ru_inblock','ru_oublock','ru_msgsnd','ru_msgrcv','ru_nsignals','ru_nvcsw','ru_nivcsw','project','department','granted_pe','slots','task_number','cpu','mem','io','category','iow','pe_taskid','maxvmem','arid','ar_sub_time')

# I don't care about any of these columns and they're just occupying memory. Drop.
myData <- myData[ -c(17:30)]
myData <- na.omit(myData)

# Some dates are before the cluster exists. Eject.
myData <- myData[!(myData$submission_time < 978307200),]
myData <- myData[!(myData$start_time < 978307200),]
myData <- myData[!(myData$end_time < 978307200),]

# How long did a job wait for? In seconds.
myData$wait_time <- (myData$start_time - myData$submission_time)

# Why not provide real dates, instead of epoch? (NTS: Should I do this on the fly at the end or include it in the frame?)
myData$realSubmission_time <- as.POSIXct(myData$submission_time,tz="UTC","1970-01-01 00:00:00")
myData$realStart_time <- as.POSIXct(myData$start_time,tz="UTC","1970-01-01 00:00:00")
myData$realEnd_time <- as.POSIXct(myData$end_time,tz="UTC","1970-01-01 00:00:00")

# I don't know a more elegant way of doing this yet.
timeData2013 <- myData[grep("2013", myData$realStart_time), ]
aggregate(data.frame(count=timeData2013$owner), list(value=timeData2013$owner), length)
timeData2014 <- myData[grep("2014", myData$realStart_time), ]
aggregate(data.frame(count=timeData2014$owner), list(value=timeData2014$owner), length)
timeData2015 <- myData[grep("2015", myData$realStart_time), ]
aggregate(data.frame(count=timeData2015$owner), list(value=timeData2015$owner), length)
timeData2016 <- myData[grep("2016", myData$realStart_time), ]
aggregate(data.frame(count=timeData2016$owner), list(value=timeData2016$owner), length)
timeData2017 <- myData[grep("2017", myData$realStart_time), ]
aggregate(data.frame(count=timeData2017$owner), list(value=timeData2017$owner), length)
timeData2018 <- myData[grep("2018", myData$realStart_time), ]
aggregate(data.frame(count=timeData2018$owner), list(value=timeData2018$owner), length)

# Cleaning raw data to get something useful. Here: runtimes
tmp <- sapply(strsplit(myData$category,"h_rt="), `[`, 2)
tmp <- sapply(strsplit(tmp,","), `[`, 1)
tmp <- sapply(strsplit(tmp," "), `[`, 1)
myData$h_rt <- as.integer(tmp)

# Actual runtime/requested runtime - the closer to 1, the more efficient that request is.
myData$time_ratio <- (myData$ru_wallclock/myData$h_rt)

# bin jobs that took longer than the max runtime of the cluster, as it's a hardware fault & can't be counted as a successful job.
myData <- myData[!(myData$ru_wallclock > 172800),]
rm (tmp)

# Cleaning for memory, this time. Fun things: sometimes h_vmem is in Mb, sometimes it's in Gb, sometimes it doesn't have an order!
tmp <- sapply(strsplit(myData$category,"h_vmem="), `[`, 2)
tmp <- sapply(strsplit(tmp,","), `[`, 1)
tmp <- sapply(strsplit(tmp," "), `[`, 1)
myData$h_vmem <- tmp
myData$memOrder <- sub('.*(?=.$)', '', myData$h_vmem, perl=T)
myData$memValue <- as.integer(gsub(".$", "", myData$h_vmem))
myData$reqMem <- ifelse(myData$memOrder == "G", (myData$reqMem <- (myData$memValue*myData$slots*1073741824)), ifelse(myData$memOrder == "M", (myData$reqMem <- myData$memValue*myData$slots*1048576),myData$memValue))
#myData$reqMem <- ifelse(myData$memOrder == "M", (myData$reqMem <- as.integer(myData$memValue*myData$slots*1048576)),myData$memOrder)
myData <- myData[!(myData$reqMem < 10),]
myData$memRatio <- (myData$maxvmem/myData$reqMem)
rm(tmp)

myData <- na.omit(myData)

# tidying up odd data. There will be a reason for these but at the moment they're discardable outliers.
myData <- myData[!(myData$time_ratio > 1),]
myData <- myData[!(myData$time_ratio <= 0),]
myData <- myData[!(myData$memRatio > 1),]
rownames(myData) <- NULL
#myData <- myData[!(myData$h_rt !='[0-9]')]
runtime <- as.data.frame(myData[,'h_rt'])
waittime <- as.data.frame(myData[,'wait_time'])
wallclock <-as.data.frame(myData[,'ru_wallclock'])

runtime$bins <- cut(myData$h_rt, breaks=48, labels=1:48,include.lowest = T)
waittime$bins <- cut(myData$wait_time, breaks=48, labels=1:48,include.lowest=T)
wallclock$bins <- cut(myData$ru_wallclock, breaks=48, labels=1:48,include.lowest=T)

# Histogram time! NOTE: Change the filenames and labels depending on which machine's data you're looking at.
# Also, the default font size is way too small. Make the graphs smaller in px or learn how to change font size.
hist.data=hist(as.numeric(wallclock$bins), plot=F)
hist.data$counts[hist.data$counts>0] <- log(hist.data$counts[hist.data$counts>0], 10)
png(filename="actual_runtime_arc2.png", width=1200,height=1600,units="px")
plot(hist.data,xlim=c(0,50), ylim=c(0,7), lwd=5,lend=2,xlab="runtime (hrs)",ylab="number of jobs(log10)",main="Actual runtime, ARC2", col="dark green")
dev.off()
rm(hist.data)
hist.data=hist(as.numeric(runtime$bins), plot=F)
hist.data$counts[hist.data$counts>0] <- log(hist.data$counts[hist.data$counts>0], 10)
png(filename="requested_runtime_arc2.png", width=1200,height=1600,units="px")
plot(hist.data,lwd=5,xlim=c(0,50), ylim=c(0,7),lend=2,xlab="runtime (hrs)",ylab="number of jobs(log10)",main="Requested runtime, ARC2", col="orange")
dev.off()
rm(hist.data)
hist.data=hist(as.numeric(waittime$bins), plot=F)
hist.data$counts[hist.data$counts>0] <- log(hist.data$counts[hist.data$counts>0], 10)
png(filename="waittime_arc2.png", width=1200,height=1600,units="px")
plot(hist.data,xlim=c(0,50), ylim=c(0,7),lwd=5,lend=2,xlab="wait time (hrs)",ylab="number of jobs(log10)",main="Job wait time, ARC2",col="dark red")
dev.off()
rm(hist.data)
png(filename="timeratio_arc2.png", width=1200,height=1600,units="px")
hist(myData$time_ratio, xlab="Wallclock:requested runtime", ylim=c(0,nrow(myData)/2),lend=2, main=paste("Wallclock vs requested runtime\n n =",nrow(myData)),col="light blue")
dev.off()
png(filename="memratio_arc2.png", width=1200,height=1600,units="px")
hist(myData$memRatio, xlab="maxvmem:requested memory",ylim=c(0,nrow(myData)/2),lend=2, main=paste("Actual vs requested memory usage\n n =",nrow(myData)),col="light green" )
dev.off()
