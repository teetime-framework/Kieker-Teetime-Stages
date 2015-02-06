package teetime.examples.traceReconstruction;

import java.util.List;

import teetime.framework.AnalysisConfiguration;
import teetime.framework.Pipeline;
import teetime.framework.Stage;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.stage.Clock;
import teetime.stage.Counter;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.InstanceOfFilter;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReaderStage;
import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.stage.trace.traceReconstruction.EventBasedTraceFactory;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;

import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;

public class TcpTraceReconstructionConf extends AnalysisConfiguration {

	private static final int MIO = 1000000;
	private static final int TCP_RELAY_MAX_SIZE = 2 * MIO;

	private final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace;

	private Counter<IMonitoringRecord> recordCounter;
	private Counter<EventBasedTrace> traceCounter;
	private ElementThroughputMeasuringStage<IFlowRecord> recordThroughputFilter;
	private ElementThroughputMeasuringStage<EventBasedTrace> traceThroughputFilter;

	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	public TcpTraceReconstructionConf() {
		intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		interThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);
		this.traceId2trace = new ConcurrentHashMapWithDefault<Long, EventBasedTrace>(EventBasedTraceFactory.INSTANCE);
		init();
	}

	private void init() {
		Pipeline<Distributor<Long>> clockStage = this.buildClockPipeline(1000);
		addThreadableStage(clockStage);

		Pipeline<Distributor<Long>> clock2Stage = this.buildClockPipeline(2000);
		addThreadableStage(clock2Stage);

		Stage pipeline = this.buildPipeline(clockStage.getLastStage(), clock2Stage.getLastStage());
		addThreadableStage(pipeline);
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		Clock clock = new Clock();
		clock.setIntervalDelayInMs(intervalDelayInMs);
		Distributor<Long> distributor = new Distributor<Long>();

		intraThreadPipeFactory.create(clock.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<Long>>(clock, distributor);
	}

	private Stage buildPipeline(final Distributor<Long> clockStage, final Distributor<Long> clock2Stage) {
		// create stages
		TcpReaderStage tcpReader = new TcpReaderStage();
		this.recordCounter = new Counter<IMonitoringRecord>();
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		this.recordThroughputFilter = new ElementThroughputMeasuringStage<IFlowRecord>();
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		this.traceThroughputFilter = new ElementThroughputMeasuringStage<EventBasedTrace>();
		this.traceCounter = new Counter<EventBasedTrace>();
		Sink<EventBasedTrace> endStage = new Sink<EventBasedTrace>();

		// connect stages
		interThreadPipeFactory.create(tcpReader.getOutputPort(), this.recordCounter.getInputPort(), TCP_RELAY_MAX_SIZE);
		intraThreadPipeFactory.create(this.recordCounter.getOutputPort(), instanceOfFilter.getInputPort());
		// intraThreadPipeFactory.create(instanceOfFilter.getOutputPort(), this.recordThroughputFilter.getInputPort());
		// intraThreadPipeFactory.create(this.recordThroughputFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		intraThreadPipeFactory.create(instanceOfFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		intraThreadPipeFactory.create(traceReconstructionFilter.getTraceValidOutputPort(), this.traceThroughputFilter.getInputPort());
		intraThreadPipeFactory.create(this.traceThroughputFilter.getOutputPort(), this.traceCounter.getInputPort());
		// intraThreadPipeFactory.create(traceReconstructionFilter.getTraceValidOutputPort(), this.traceCounter.getInputPort());
		intraThreadPipeFactory.create(this.traceCounter.getOutputPort(), endStage.getInputPort());

		interThreadPipeFactory.create(clockStage.getNewOutputPort(), this.recordThroughputFilter.getTriggerInputPort(), 10);
		interThreadPipeFactory.create(clock2Stage.getNewOutputPort(), this.traceThroughputFilter.getTriggerInputPort(), 10);

		return tcpReader;
	}

	public int getNumRecords() {
		return this.recordCounter.getNumElementsPassed();
	}

	public int getNumTraces() {
		return this.traceCounter.getNumElementsPassed();
	}

	public List<Long> getRecordThroughputs() {
		return this.recordThroughputFilter.getThroughputs();
	}

	public List<Long> getTraceThroughputs() {
		return this.traceThroughputFilter.getThroughputs();
	}

}
