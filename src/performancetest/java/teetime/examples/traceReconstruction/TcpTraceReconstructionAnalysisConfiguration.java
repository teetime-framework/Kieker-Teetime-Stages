package teetime.examples.traceReconstruction;

import java.util.LinkedList;
import java.util.List;

import teetime.framework.AnalysisConfiguration;
import teetime.framework.IStage;
import teetime.framework.RunnableStage;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.stage.Clock;
import teetime.stage.Counter;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.InstanceOfFilter;
import teetime.stage.Pipeline;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReader;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;
import teetime.util.concurrent.hashmap.TraceBuffer;

import kieker.analysis.plugin.filter.flow.TraceEventRecords;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;

public class TcpTraceReconstructionAnalysisConfiguration extends AnalysisConfiguration {

	private static final int MIO = 1000000;
	private static final int TCP_RELAY_MAX_SIZE = 2 * MIO;

	private final List<TraceEventRecords> elementCollection = new LinkedList<TraceEventRecords>();

	private Thread clockThread;
	private Thread clock2Thread;
	private Thread workerThread;

	private final ConcurrentHashMapWithDefault<Long, TraceBuffer> traceId2trace = new ConcurrentHashMapWithDefault<Long, TraceBuffer>(new TraceBuffer());

	private Counter<IMonitoringRecord> recordCounter;
	private Counter<TraceEventRecords> traceCounter;
	private ElementThroughputMeasuringStage<IFlowRecord> recordThroughputFilter;
	private ElementThroughputMeasuringStage<TraceEventRecords> traceThroughputFilter;

	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	public TcpTraceReconstructionAnalysisConfiguration() {
		intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		interThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);
		init();
	}

	private void init() {
		Pipeline<Distributor<Long>> clockStage = this.buildClockPipeline(1000);
		this.clockThread = new Thread(new RunnableStage(clockStage));

		Pipeline<Distributor<Long>> clock2Stage = this.buildClockPipeline(2000);
		this.clock2Thread = new Thread(new RunnableStage(clock2Stage));

		IStage pipeline = this.buildPipeline(clockStage.getLastStage(), clock2Stage.getLastStage());
		this.workerThread = new Thread(new RunnableStage(pipeline));
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		Clock clock = new Clock();
		clock.setIntervalDelayInMs(intervalDelayInMs);
		Distributor<Long> distributor = new Distributor<Long>();

		intraThreadPipeFactory.create(clock.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<Long>>(clock, distributor);
	}

	private IStage buildPipeline(final Distributor<Long> clockStage, final Distributor<Long> clock2Stage) {
		// create stages
		TcpReader tcpReader = new TcpReader();
		this.recordCounter = new Counter<IMonitoringRecord>();
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		this.recordThroughputFilter = new ElementThroughputMeasuringStage<IFlowRecord>();
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		this.traceThroughputFilter = new ElementThroughputMeasuringStage<TraceEventRecords>();
		this.traceCounter = new Counter<TraceEventRecords>();
		Sink<TraceEventRecords> endStage = new Sink<TraceEventRecords>();

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

	public List<TraceEventRecords> getElementCollection() {
		return this.elementCollection;
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
