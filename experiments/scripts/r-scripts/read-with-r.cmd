library("data.table")

data=fread("data.csv")
column2=data[,V2]		# [where,select,groupby]


q = quote(list(x,sd(y),mean(y*z)))
DT[,eval(q)] # identical to DT[,list(x,sd(y),mean(y*z))]

### Use the aggregate function to split and get the mean of the data
aggregate(data$data,list(data$names),mean)
 
### Use the sapply and split functions to do the same thing
s <- split(data$data,list(data$names))
sapply(s,mean) 