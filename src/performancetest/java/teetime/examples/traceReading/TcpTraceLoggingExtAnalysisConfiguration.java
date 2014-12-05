package teetime.examples.traceReading;

import java.util.List;

import teetime.framework.AnalysisConfiguration;
import teetime.framework.IStage;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.stage.Clock;
import teetime.stage.Counter;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.Pipeline;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReader;

import kieker.common.record.IMonitoringRecord;

public class TcpTraceLoggingExtAnalysisConfiguration extends AnalysisConfiguration {

	private Counter<IMonitoringRecord> recordCounter;
	private ElementThroughputMeasuringStage<IMonitoringRecord> recordThroughputStage;
	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	public TcpTraceLoggingExtAnalysisConfiguration() {
		intraThreadPipeFactory = PipeFactoryRegistry.INSTANCE.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		interThreadPipeFactory = PipeFactoryRegistry.INSTANCE.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);

		final Pipeline<Distributor<Long>> clockPipeline = this.buildClockPipeline(1000);
		addThreadableStage(clockPipeline);
		final IStage tcpPipeline = this.buildTcpPipeline(clockPipeline.getLastStage());
		addThreadableStage(tcpPipeline);
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		Clock clockStage = new Clock();
		clockStage.setInitialDelayInMs(intervalDelayInMs);
		clockStage.setIntervalDelayInMs(intervalDelayInMs);
		Distributor<Long> distributor = new Distributor<Long>();

		intraThreadPipeFactory.create(clockStage.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<Long>>(clockStage, distributor);
	}

	private IStage buildTcpPipeline(final Distributor<Long> previousClockStage) {
		TcpReader tcpReader = new TcpReader();
		this.recordCounter = new Counter<IMonitoringRecord>();
		this.recordThroughputStage = new ElementThroughputMeasuringStage<IMonitoringRecord>();
		Sink<IMonitoringRecord> endStage = new Sink<IMonitoringRecord>();

		intraThreadPipeFactory.create(tcpReader.getOutputPort(), this.recordCounter.getInputPort());
		intraThreadPipeFactory.create(this.recordCounter.getOutputPort(), this.recordThroughputStage.getInputPort());
		intraThreadPipeFactory.create(this.recordThroughputStage.getOutputPort(), endStage.getInputPort());
		// intraThreadPipeFactory.create(this.recordCounter.getOutputPort(), endStage.getInputPort());

		interThreadPipeFactory.create(previousClockStage.getNewOutputPort(), this.recordThroughputStage.getTriggerInputPort(), 10);

		return tcpReader;
	}

	public int getNumRecords() {
		return this.recordCounter.getNumElementsPassed();
	}

	public List<Long> getRecordThroughputs() {
		return this.recordThroughputStage.getThroughputs();
	}

}
