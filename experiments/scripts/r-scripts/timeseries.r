library("data.table")
require(graphics)			# ts.plot

loadResponseTimesInNs = function(filename, numFirstValuesToIgnore) {
	csvTable = fread(filename, skip=numFirstValuesToIgnore)
	values = csvTable[,V2]
	return(values)
}

getChunkedYvalues = function(values, numChunks) {
	mymatrix = matrix(values, ncol=numChunks)		# (<numChunks> of columns; first column contains the first sequence of values
	meanRespTimes = colMeans(mymatrix)				# (mean of <numChunks> micro secs) per column
	return(meanRespTimes)
}

getFilename = function(name, iteration, stackDepth, scenario) {
	s = paste(name, "-", iteration, "-", stackDepth, "-", scenario, ".csv", sep="")
	return(s)
}

getConfidenceInterval = function(values) {
	stdDeviation	<- sd(values)
	numElements		<- length(values)
	# a confidence level of 95% corresponds to the argument value 0.975
	ci = qnorm(0.975)*stdDeviation/sqrt(numElements)
	return(ci)
}

nano2micro = function(value) {
	return(value/(1000))
}

micro2sec = function(value) {
	return(value/(1000*1000))
}

name			<- "h:/results-benchmark-teetime/raw"
iterations		<- 1:5		# 1:10
stackDepth		<- 10
scenarios		<- 3:6

numFirstValuesToIgnore	<- 1000*1000

numXvalues			<- 1000
timeseries 			<- matrix(nrow=numXvalues,ncol=length(scenarios))
meanValues			= scenarios
confidenceInterval	= scenarios
quantiles 			<- matrix(nrow=5,ncol=length(scenarios))
durationsInSec		= scenarios
throughputValues	= scenarios

#timeseriesColors	<- c("black","black","black","red","blue","green")
timeseriesColors	<- c(rgb(0,0,0), rgb(0.3,0.3,0.9), rgb(0.3,0.9,0.3), rgb(0.3,0.9,1))

rowNames				<- formatC( c("mean","ci95%","min","25%","median","75%","max","duration (sec)","throughput (per sec)"), format="f", width=20)
#colNames				<- c("no instrumentation","instrumentation","collecting","record recon","trace recon", "trace reduc")
colNames				<- c("collecting","record recon","trace recon", "trace reduc")
printMatrixDimnames		<- list(rowNames, colNames)
printMatrix				<- matrix(nrow=length(rowNames), ncol=length(scenarios), dimnames=printMatrixDimnames)
resultTablesFilename	<- "resultTables.txt"

outputIterationResults = function(iteration) {
	scenarioIndex = 0
	for (scenario in scenarios) {
		print(paste("iteration:", iteration, ", ", "scenario:", scenario, sep=""))
		flush.console()
		
		scenarioIndex			= scenarioIndex+1

		filename				<- getFilename(name, iteration, stackDepth, scenario)
		respTimesInNs			<- loadResponseTimesInNs(filename, numFirstValuesToIgnore)
		numValues				<- length(respTimesInNs)
		chunkedRespTimesInNs	<- getChunkedYvalues(respTimesInNs, numXvalues)
		chunkedRespTimesInUs	<- nano2micro( chunkedRespTimesInNs )

		timeseries[,scenarioIndex] <- ts(chunkedRespTimesInUs)

		meanValues[scenarioIndex] 			<- mean(chunkedRespTimesInUs)
		confidenceInterval[scenarioIndex] 	<- getConfidenceInterval(chunkedRespTimesInUs)
		quantiles[,scenarioIndex] 			<- quantile(chunkedRespTimesInUs, names=FALSE)
		durationsInSec[scenarioIndex]		<- micro2sec( numValues*meanValues[scenarioIndex] )
		throughputValues[scenarioIndex]		<- numValues/durationsInSec[scenarioIndex]
	}

	ts.plot(timeseries, gpars = list(), col = timeseriesColors)

	printMatrix[1,]		<- formatC(meanValues, format="f", digits=4, width=20)
	printMatrix[2,]		<- formatC(confidenceInterval, format="f", digits=4, width=20)
	for (qIndex in 1:nrow(quantiles)) {
		printMatrix[(2+qIndex),]	<- formatC(quantiles[qIndex,], format="f", digits=4, width=20)
	}
	printMatrix[(2+nrow(quantiles)+1),]	<- formatC(durationsInSec, format="f", digits=4, width=20)
	printMatrix[(2+nrow(quantiles)+2),]	<- formatC(throughputValues, format="f", digits=4, width=20)
	
	write.table(printMatrix,file=resultTablesFilename,append=TRUE,quote=FALSE,sep="\t",col.names=FALSE)
	write("\n\n", file=resultTablesFilename, append = TRUE)
}

for (iteration in iterations) {
	outputIterationResults(iteration)
}
