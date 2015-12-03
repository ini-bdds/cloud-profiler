#!/usr/bin/Rscript
library(ggplot2)
require(reshape2)
library(RPostgreSQL)


files = list.files(pattern="*.csv")

dataframe <- NULL

p <- ggplot()


print ('filename & memory used (GB) & memory used percent & mean cpu idle & mean cpu user & mean cpu sys & mean cpu wait & mean cpu guest & disk read (GB) & disk write (GB)')
for (filename in files){
  input = read.table(filename, sep = ",", header = T)
  # Trim out any rows with a question mark
  input[input == '?'] <- NA
  input <- input[complete.cases(input),]

  # Get memory stats
  maxmem <- max(as.numeric(as.character(input$mem.freemem))) / 1024
  minmem <- min(as.numeric(as.character(input$mem.freemem))) / 1024
  input$FreeMemPercent <- as.numeric(as.character(input$mem.freemem)) / as.numeric(as.character(input$mem.physmem)) * 100
  maxmemper <- max(input$FreeMemPercent)
  minmemper <- min(input$FreeMemPercent)

  mem_str <- paste(filename, maxmem - minmem, maxmemper - minmemper, sep = ' & ')
  # print(maxmem)
  # print(minmem)
  # print(maxmem - minmem)
  # print(maxmemper)
  # print(minmemper)
  # print(maxmemper - minmemper)



  # Now print the cpu usage - i should divide by 100 and divide by # threads
  # kernel.all.cpu.idle,kernel.all.cpu.user,kernel.all.cpu.sys,kernel.all.cpu.wait.total,kernel.all.cpu.nice,kernel.all.cpu.guest,
  idle <- mean(as.numeric(as.character(input$kernel.all.cpu.idle)))
  user <- mean(as.numeric(as.character(input$kernel.all.cpu.user)))
  sys <- mean(as.numeric(as.character(input$kernel.all.cpu.sys)))
  wait <- mean(as.numeric(as.character(input$kernel.all.cpu.wait)))
  guest <- mean(as.numeric(as.character(input$kernel.all.cpu.gues)))
  
  cpu_str <- paste(idle, user, sys, wait, guest, sep=" & ")
  # print(idle)
  # print(user)
  # print(sys)
  # print(wait)
  # print(guest)


  # Sum up the disk bytes read/written

  read <- sum(as.numeric(as.character(input$disk.all.read_bytes))) / 1024
  write <- sum(as.numeric(as.character(input$disk.all.write_bytes))) / 1024
  
  disk_str <- paste(read, write, sep=" & ")
  # print(read)
  # print(write)


  print(paste(mem_str, cpu_str, disk_str, sep=" & "))



}

dev.off()
exit()


# Plot execution time from the database



drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname="profiler",host="",port=5432,user="profiler",password="")

tools <- c('fastqc', 'bowtie', 'markdups')

# This query gets all of the completed jobs (doubles up on some types etc.)
#start_query <- "select profile_job.*, workload.executable, instance_type.type from profile_job, workload, work_instance, instance_type where profile_job.workload_id = workload.id and profile_job.work_instance_id = work_instance.id and work_instance.type = instance_type.id and status = 'Complete' and workload.executable like '%"
#end_query <- "%' order by id desc;"

# This query just averages the execution time. It will keep those taht actually failed, so i need to curate this data.
start_query <- "select instance_type.type, avg(profile_job.execution_time) as execution_time from profile_job, workload, work_instance, instance_type where profile_job.workload_id = workload.id and profile_job.work_instance_id = work_instance.id and work_instance.type = instance_type.id and profile_job.worked = True and status = 'Complete' and workload.executable like '%"
end_query <- "%' group by instance_type.type;"

# for (tool in tools){
# Not too sure why, but the dbGetQuery command doesn't seem to like being in a loop. Perhaps it needs to be called as a function.
query <- paste(start_query, 'fastqc', end_query, sep='')
rs <- dbGetQuery(con, query)
fastqc <- ggplot(rs, aes(x = as.factor(rs$type), y = rs$execution_time, fill=as.factor(rs$type))) + 
          geom_bar(stat = "identity") + 
          #ggtitle('FastQC') + 
          ylab('Time (s)') + scale_y_continuous(limits=c(0, 20000)) +
          xlab('Instance Type') + 
      theme(panel.background = element_rect(fill='white', colour='black'), axis.text.x = element_text(angle = 70, hjust = 1), legend.position = "none")

ggsave(fastqc, file="FastQC-exec.pdf", height=175, units='mm', device=cairo_pdf)

query <- paste(start_query, 'bowtie', end_query, sep='')
rs <- dbGetQuery(con, query)
bowtie <- ggplot(rs, aes(x = as.factor(rs$type), y = rs$execution_time, fill=as.factor(rs$type))) + 
          geom_bar(stat = "identity") + 
          #ggtitle('Bowtie') + 
          ylab('Time (s)') + scale_y_continuous(limits=c(0, 20000)) +
          xlab('Instance Type') + 
      theme(panel.background = element_rect(fill='white', colour='black'), axis.text.x = element_text(angle = 70, hjust = 1), legend.position = "none")
ggsave(bowtie, file="Bowtie-exec.pdf", height=175, units='mm', device=cairo_pdf)

query <- paste(start_query, 'markdups', end_query, sep='')
rs <- dbGetQuery(con, query)
markdups <- ggplot(rs, aes(x = as.factor(rs$type), y = rs$execution_time, fill=as.factor(rs$type))) +
            geom_bar(stat = "identity") +
            #ggtitle('MarkDups') + 
            ylab('Time (s)') + scale_y_continuous(limits=c(0, 20000)) +
            xlab('Instance Type') + 
        theme(panel.background = element_rect(fill='white', colour='black'), axis.text.x = element_text(angle = 70, hjust = 1), legend.position = "none")
ggsave(markdups, file="Markdups-exec.pdf", height=175, units='mm', device=cairo_pdf)

query <- paste(start_query, 'map', end_query, sep='')
rs <- dbGetQuery(con, query)
mapbwa <- ggplot(rs, aes(x = as.factor(rs$type), y = rs$execution_time, fill=as.factor(rs$type))) +
          geom_bar(stat = "identity") + 
          #ggtitle('MapBWAIllumina') + 
          ylab('Time (s)') + scale_y_continuous(limits=c(0, 20000)) +
          xlab('Instance Type') + 
      theme(panel.background = element_rect(fill='white', colour='black'), axis.text.x = element_text(angle = 70, hjust = 1), legend.position = "none")
ggsave(mapbwa, file="MapBWA-exec.pdf", height=175, units='mm', device=cairo_pdf)

query <- paste(start_query, 'mem', end_query, sep='')
rs <- dbGetQuery(con, query)
bwamem <- ggplot(rs, aes(x = as.factor(rs$type), y = rs$execution_time, fill=as.factor(rs$type))) + 
          geom_bar(stat = "identity") + 
          #ggtitle('BWA Mem') + 
          ylab('Time (s)') + scale_y_continuous(limits=c(0, 20000)) +
          xlab('Instance Type') + 
      theme(panel.background = element_rect(fill='white', colour='black'), axis.text.x = element_text(angle = 70, hjust = 1), legend.position = "none")
ggsave(bwamem, file="BWAMem-exec.pdf", height=175, units='mm', device=cairo_pdf)

# }



#
#head(test)
dbDisconnect(con)


dev.off()
exit()





# Plot CPU utilization


files = list.files(pattern="*MapBWAIllumina*")

dataframe <- NULL

p <- ggplot()

for (filename in files){
	input = read.table(filename, sep = ",", header = T)
	# Trim out any rows with a question mark
	input[input == '?'] <- NA
	input <- input[complete.cases(input),]

	# Fix the time stamp
	input$Time <- as.POSIXct(input$Time, format="%a %b %d %H:%M:%S")
	input$Time <- as.numeric(input$Time)
	input$Time <- as.POSIXct(input$Time, origin="1970-01-01")

	# Work out what we should divide by for percentage

  if(length(grep("r3.xlarge",filename))>0) cpu_total <- 4000
  if(length(grep("r3.2xlarge",filename))>0) cpu_total <- 8000
  if(length(grep("r3.4xlarge",filename))>0) cpu_total <- 16000
  if(length(grep("r3.8xlarge",filename))>0) cpu_total <- 32000
  print(filename)

  head(input$kernel.all.cpu.user)
  # Calculate the percentage of cpu usage
	input$CPUPercent <- as.numeric(as.character(input$kernel.all.cpu.user)) / cpu_total * 100
	input$CPUPercent <- as.factor(input$CPUPercent)
  # print(input$CPUPercent)
  head(input$CPUPercent)
	input$TimeNum <- as.factor(as.numeric(as.factor(input$Time)))
	head(input$TimeNum)

  # Melt the data to Time <something> pairs
	df <- melt(input,  id.vars = 'Time', variable.name = 'series')
	df$value <- as.character(df$value)
	df$value <- as.numeric(df$value)
  # print(df)


  head(df)
	tmp <- subset(df, grepl("CPUPercent", series))
  # print(tmp)
	newfilename <- filename
	if(length(grep("r3.xlarge",filename))>0) newfilename <- 'r3.xlarge'
	if(length(grep("r3.2xlarge",filename))>0) newfilename <- 'r3.2xlarge'
	if(length(grep("r3.4xlarge",filename))>0) newfilename <- 'r3.4xlarge'
	if(length(grep("r3.8xlarge",filename))>0) newfilename <- 'r3.8xlarge'
	tmp$timeid <- as.numeric(as.factor(tmp$Time))
	tmp$Instance <- newfilename
	dataframe <- rbind(dataframe, tmp)

}


mem <- ggplot() + geom_line(data=dataframe, aes(y=value,x=timeid, group=Instance, colour=Instance)) + 
       xlab('Time (s)') +
       ylab('Percent CPU Utilization') + scale_y_continuous(limits=c(0, 100)) +
       theme(panel.background = element_rect(fill='white', colour='black')) # , legend.position = c(0.85, 0.85)


ggsave(mem, file="MapBWAIllumina-cpu.pdf", height=175, units='mm', device=cairo_pdf)

dev.off()
exit()





# input = read.table("MarkDups-3387-r3.2xlarge-memory=60000m.csv", sep = ",", header = T)
# 	input[input == '?'] <- NA
# 	input <- input[complete.cases(input),]
# 	#input$FreeMemPercent <- as.factor(input$FreeMemPercent)

# 	head(input$mem.freemem)
# 	head(input$mem.physmem)
# 	input$FreeMemPercent <- as.numeric(as.character(input$mem.freemem)) / as.numeric(as.character(input$mem.physmem)) * 100

# 	input$Time <- as.POSIXct(input$Time, format="%a %b %d %H:%M:%S")
# 	input$Time <- as.numeric(input$Time)
# 	input$Time <- as.POSIXct(input$Time, origin="1970-01-01")



# Print the table details of each file, so total disk written, difference between max/min free memory (and as %), average cpu idle/user/system of each one



# plot_ecdf <- function(ecdf_data_frame,varY){
#   # takes in full data frame as df, varX as column name, varY as column name
#   sort_incoming_df <- sort(ecdf_data_frame[,varY])
#   #n is the count of entires with all NA eliminated (sanity check)
#   n = sum(!is.na(sort_incoming_df))
#   #p <- qplot(data=ecdf_data_frame, x=varX,y=varY)
#   #enable postscript for output as eps
#   #p <- postscript(paste(varY, ".eps", sep=""), width=480, height=480)
#   return sort_incoming_df
#   head(sort_incoming_df)
#   plot(sort_incoming_df, (1:n)/n, type = 's', ylim = c(0, 1), xlab = paste('Sample Quantiles of ', varY,sep=""), ylab = '', main = paste('ECDF ', varY,sep= ""))
#   # enable if you want to do level check 
#   # abline(v = 62.5, h = 0.75)
#   # create a matrix of titles vs. plots - and plot all in one go. Not advisable for such big datasets.
#   # plotholders[[1,length(plotholders[1]) + 1]] = varY
#   # plotholders[[2,length(plotholders[2]) + 1]] = p
#   # dev.off() 

#   # head(sort_incoming_df)
#   # p <- ggplot(sort_incoming_df, y= (1:n)/n)
#   # plot(p)
#   # , (1:n)/n, type = 's', ylim = c(0, 1), xlab = paste('Sample Quantiles of ', varY,sep=""), ylab = '', main = paste('ECDF ', varY,sep= ""))
  
# }

# plot_measure <- function(input_df,VarY){
#   # VarY is the measure of interest, input_df is the input data frame
#   p <- plot_ecdf(input_df,paste(VarY))
  
#   print(p)  
#   print("Done")
# }

# plot_file <- function(input_file){
#   tmp_file_df <-  na.omit(read.csv(input_file,header = TRUE,na.strings='?'))     
#   #lapply(get_col_names(tmp_file_df),plot_measure,input_df=tmp_file_df)
#   lapply('kernel.all.cpu.user',plot_measure,input_df=tmp_file_df)
# }


# files = list.files(pattern="*8xlarge*")


# print(files)
# lapply(files, plot_file)

# dev.off()
# exit()

library(magrittr)
library(dplyr)
library(data.table)
library(ggplot2)


change_name <- function(filename){
	x <- NULL
	if(length(grep("Bowtie",filename))>0) x <- 'Bowtie'
	if(length(grep("FastQC",filename))>0) x <- 'FastQC'
	if(length(grep("BWA_MEM",filename))>0) x <- 'BWA Mem'
	if(length(grep("Illumina",filename))>0) x <- 'Map BWA Illumina'
    if(length(grep("Mark",filename))>0) x <- 'Mark Dups'
    print(x)
    return(x)
}

# Normalized memory and time
plot_mem_t <- function(a){
  list.files(pattern=a) %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
    mutate(Workload = change_name(x))
  	}) %>%
  rbindlist %>%
  select(Workload, Time, mem.freemem) %>%
  filter(!is.na(mem.freemem)) %>%
  group_by(Workload) %>%
  mutate(total = sort(mem.freemem)/251902420,
  	     proportion = (1:n())/n()
  	     ) %>%
  	ggplot(aes(total, proportion,colour=Workload)) + geom_line() +xlab('Memory Utilization') +ylab("ECDF") + theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.2, 0.85)) %>%
  	return
}

# normalized memory without normalized time
plot_mem <- function(a){
  list.files(pattern=a) %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
    mutate(Workload = change_name(x))
  	}) %>%
  rbindlist %>%
  select(Workload, Time, mem.freemem) %>%
  filter(!is.na(mem.freemem)) %>%
  group_by(Workload) %>%
  mutate(total = sort(mem.freemem)/251902420,
  	     proportion = (1:n())
  	     ) %>%
  	ggplot(aes(total, proportion,colour=Workload)) + geom_line()+xlab('Memory Utilization') +ylab("ECDF")  + theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.2, 0.85)) %>%
  	return
}

# memory amounts:
# 8x = 251902420
# 4x = 125903992
# 2x = 62916356


p <- plot_mem_t('*.8xlarge*')
plot(p)
ggsave(p, file="FreeMem-8x-norm.pdf", height=175, units='mm', device=cairo_pdf)

p <- plot_mem('*.8xlarge*')
plot(p)
ggsave(p, file="FreeMem-8x.pdf", height=175, units='mm', device=cairo_pdf)



#######################################CPU##############################


# Normalized cpu and time
plot_cpu_norm <- function(a){
	list.files(pattern=a) %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = change_name(x))
  	}) %>%
  rbindlist %>%
  select(source, Time, kernel.all.cpu.user) %>%
  filter(!is.na(kernel.all.cpu.user)) %>%
  group_by(source) %>%
  mutate(total = sort(kernel.all.cpu.user)/32000,
  	     proportion = (1:n())/n()
  	     ) %>%
  ggplot(aes(total, proportion,colour=source)) + 
  geom_line() +xlab('CPU Utilization') +ylab("ECDF") +
  theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2)) %>%
  return
}

# normalize cpu but with real time
plot_cpu_t <- function(a){
	list.files(pattern=a) %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = change_name(x))
  	}) %>%
  lapply(na.omit) %>%
  rbindlist %>%
  select(source, Time, kernel.all.cpu.user) %>%
  group_by(source) %>%
  mutate(total = sum(kernel.all.cpu.user,na.rm=T),
  	     proportion = cumsum(kernel.all.cpu.user)/32000,
  	     time_x = (1:n())
  	     ) %>%
  ggplot(aes(time_x, proportion,colour=source)) + 
  geom_line() +xlab('Time') +ylab("Cumulative CPU Utilization") +
  theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2)) %>%
  return
}

#To change plot make sure you change the file filter below, save file name, and the memory/cpu amount divided by to normalize
p <- plot_cpu_norm('*.8xlarge*')
plot(p)
ggsave(p, file="CPU-8x-norm.pdf", height=175, units='mm', device=cairo_pdf)

p <- plot_cpu_t('*.8xlarge*')
plot(p)
ggsave(p, file="CPU-8x-time.pdf", height=175, units='mm', device=cairo_pdf)


dev.off()
exit()




## Over time 
list.files(pattern="*8xlarge*") %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = x)
  	}) %>%
  lapply(na.omit) %>%
  rbindlist %>%
  select(source, Time, kernel.all.cpu.user) %>%
  group_by(source) %>%
  mutate(total = sum(kernel.all.cpu.user,na.rm=T),
  	     proportion = cumsum(kernel.all.cpu.user)/32000,
  	     time_x = (1:n())
  	     ) %>%
  ggplot(aes(time_x, proportion,colour=source)) + 
  geom_line() +
  theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2))



## how much work is done how much
list.files(pattern="*8xlarge*") %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = x)
  	}) %>%
  rbindlist %>%
  select(source, Time, kernel.all.cpu.user) %>%
  filter(!is.na(kernel.all.cpu.user)) %>%
  group_by(source) %>%
  mutate(total = sort(kernel.all.cpu.user),
  	     proportion = (1:n())/n()
  	     ) %>%
  ggplot(aes(total, proportion,colour=source)) + 
  geom_line() +
  theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2))



## how much work is done how much normalised
list.files(pattern="*8xlarge*") %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = x)
  	}) %>%
  rbindlist %>%
  select(source, Time, kernel.all.cpu.user) %>%
  filter(!is.na(kernel.all.cpu.user)) %>%
  group_by(source) %>%
  mutate(total = sort(kernel.all.cpu.user)/32000,
  	     proportion = (1:n())/n()
  	     ) %>%
  ggplot(aes(total, proportion,colour=source)) + 
  geom_line() +
  theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2))



# cumsum'd bytes read from disk
list.files(pattern="*.4xlarge*") %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = x)
  	}) %>%
  lapply(na.omit) %>%
  rbindlist %>%
  select(source, Time, disk.all.read_bytes) %>%
  group_by(source) %>%
  mutate(total = sum(disk.all.read_bytes,na.rm=T),
  	     proportion = cumsum(disk.all.read_bytes),#/total,
  	     time_x = (1:n())
  	     ) %>%
  ggplot(aes(time_x, proportion,colour=source)) + 
  geom_line() +
  theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2))


# cumsum'd bytes written to disk
list.files(pattern="*8xlarge*") %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = x)
  	}) %>%
  lapply(na.omit) %>%
  rbindlist %>%
  select(source, Time, disk.all.write_bytes) %>%
  group_by(source) %>%
  mutate(total = sum(disk.all.write_bytes,na.rm=T),
  	     proportion = cumsum(disk.all.write_bytes)/total,
  	     time_x = (1:n())
  	     ) %>%
  ggplot(aes(time_x, proportion,colour=source)) + 
  geom_line() +
  theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2))



# Memory CDF normalized

list.files(pattern="*8xlarge*") %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = x)
  	}) %>%
  rbindlist %>%
  select(source, Time, mem.freemem) %>%
  filter(!is.na(mem.freemem)) %>%
  group_by(source) %>%
  mutate(total = sort(mem.freemem)/240000,
  	     proportion = (1:n())/n()
  	     ) %>%
  	ggplot(aes(total, proportion,colour=source)) + 
    geom_line() +
    theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2))
  	


#gg save stuff


plot_stuff <- function(a){
  list.files(pattern="*8xlarge*") %T>%
  print %>%
  lapply(function(x) {
  	fread(x,header=TRUE, na.strings="?") %>%
  	mutate(source = x)
  	}) %>%
  rbindlist %>%
  select(source, Time, mem.freemem) %>%
  filter(!is.na(mem.freemem)) %>%
  group_by(source) %>%
  mutate(total = sort(mem.freemem)/240000,
  	     proportion = (1:n())/n()
  	     ) %>%
  	ggplot(aes(total, proportion,colour=source)) + geom_line() + theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.2)) %>%
  	return
}

p <- plot_stuff('a')
plot(p)
ggsave(p, file="test.pdf", height=175, units='mm', device=cairo_pdf)










# Plot memory utilization


files = list.files(pattern="*.csv")

dataframe <- NULL

p <- ggplot()

for (filename in files){
	input = read.table(filename, sep = ",", header = T)
	# Trim out any rows with a question mark
	input[input == '?'] <- NA
	input <- input[complete.cases(input),]

	# Fix the time stamp
	input$Time <- as.POSIXct(input$Time, format="%a %b %d %H:%M:%S")
	input$Time <- as.numeric(input$Time)
	input$Time <- as.POSIXct(input$Time, origin="1970-01-01")

	# Plot the memory usage as a percentage
	input$FreeMemPercent <- as.numeric(as.character(input$mem.freemem)) / as.numeric(as.character(input$mem.physmem)) * 100
	input$FreeMemPercent <- as.factor(input$FreeMemPercent)

	input$TimeNum <- as.factor(as.numeric(as.factor(input$Time)))
	head(input$TimeNum)

	df <- melt(input,  id.vars = 'Time', variable.name = 'series')
	df$value <- as.character(df$value)
	df$value <- as.numeric(df$value)
	head(df)

	cat(filename)
    
	tmp <- subset(df, grepl("FreeMemPercent", variable))
	newfilename <- filename
	if(length(grep("r3.xlarge",filename))>0) newfilename <- 'r3.xlarge'
	if(length(grep("r3.2xlarge",filename))>0) newfilename <- 'r3.2xlarge'
	if(length(grep("r3.4xlarge",filename))>0) newfilename <- 'r3.4xlarge'
	if(length(grep("r3.8xlarge",filename))>0) newfilename <- 'r3.8xlarge'
	tmp$timeid <- as.numeric(as.factor(tmp$Time))
	tmp$Instance <- newfilename
	dataframe <- rbind(dataframe, tmp)

}


mem <- ggplot() + geom_line(data=dataframe, aes(y=value,x=timeid, group=Instance, colour=Instance)) + 
       xlab('Time (s)') +
       ylab('Percent Free Memory') + 
       theme(panel.background = element_rect(fill='white', colour='black'), legend.position = c(0.85, 0.85))


ggsave(mem, file="MapBWA-memory.pdf", height=175, units='mm', device=cairo_pdf)

dev.off()
exit()
















# 	exit()


plot_vars <- function(filename) {
	cat(filename)
	tryCatch({
	input = read.table(filename, sep = ",", header = T)

	# Trim out any rows with a question mark
	input[input == '?'] <- NA
	input <- input[complete.cases(input),]

	# Fix the time stamp
	input$Time <- as.POSIXct(input$Time, format="%a %b %d %H:%M:%S")
	input$Time <- as.numeric(input$Time)
	input$Time <- as.POSIXct(input$Time, origin="1970-01-01")


	# Melt the data against time
	df <- melt(input,  id.vars = 'Time', variable.name = 'series')

	# Make it so all of the values are numeric (otherwise they are not plotted on the axes in order)
	df$value <- as.character(df$value)
	df$value <- as.numeric(df$value)


	# Plot each of the shared variables (cpu.idle<x> etc.) on an individual plot
	plot_names_list = list()
	plot_list = list()

	# Plot all columns
	for (col in names(input)){
		if (col != 'Time'){
			var_name = col
			if (grepl("*[0-9]+", var_name)){
				var_name = substr(col, 0, nchar(col) - 1)
			}
			cat(var_name)
			cat("\n")
			# Now plot it if it hasn't already been plotted
			if (!var_name %in% plot_names_list){
				p <- ggplot(data=subset(df, grepl(var_name, variable)), aes(Time, value, group=variable, colour=variable)) + geom_line() + ggtitle(var_name)
				plot_names_list[[length(plot_names_list) + 1]] = var_name
				plot_list[[length(plot_list) + 1]] = p
			}
			# p <- ggplot(data=subset(df, variable==col), aes(Time, value, group=variable)) 
			# plot_list[[length(plot_list) + 1]] = p + geom_line() + ggtitle(col)
			
		}
	}
    
	# Plot the memory usage as a percentage
	input$FreeMemPercent <- as.numeric(as.character(input$mem.freemem)) / as.numeric(as.character(input$mem.physmem)) * 100
	input$FreeMemPercent <- as.factor(input$FreeMemPercent)

	input$TimeNum <- as.factor(as.numeric(as.factor(input$Time)))
	head(input$TimeNum)

	df <- melt(input,  id.vars = 'Time', variable.name = 'series')
	df$value <- as.character(df$value)
	df$value <- as.numeric(df$value)
	head(df)

	p <- ggplot(data=subset(df, grepl("FreeMemPercent", variable)), aes(as.numeric(as.factor(Time)), value, group=variable, colour=variable)) + geom_line() + ggtitle("Memory as percent")
	plot_names_list[[length(plot_names_list) + 1]] = "Memory as percent"
	plot_list[[length(plot_list) + 1]] = p


	# Save the plots in a file
	pdf(paste(filename, ".pdf", sep=""))
	for(i in 1:length(plot_list)){
		plot(plot_list[[i]])
	}

	dev.off()

	})
}


# Plot all of the variables we have recorded.
# for (f in files){
# 	tmp <- plot_vars(f)
# }










dev.off()


# gg_color_hue <- function(n) {
#   hues = seq(15, 375, length=n+1)
#   hcl(h=hues, l=65, c=100)[1:n]
# }

# ggsave(cost_together, file="JobCost.pdf", height=125, units='mm', device=cairo_pdf)
