package experiment.fse15.teetime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import teetime.framework.Analysis;
import teetime.framework.AnalysisConfiguration;
import teetime.framework.Pipeline;
import teetime.framework.Stage;
import teetime.framework.pipe.IPipe;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.framework.pipe.SpScPipe;
import teetime.stage.Clock;
import teetime.stage.InstanceOfFilter;
import teetime.stage.Relay;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.EveryXthPrinter;
import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.stage.trace.traceReconstruction.EventBasedTraceFactory;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.stage.trace.traceReduction.EventBasedTraceComperator;
import teetime.stage.trace.traceReduction.TraceAggregationBuffer;
import teetime.stage.trace.traceReduction.TraceReductionFilter;
import teetime.util.Pair;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;

import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;

class TcpTraceReductionTeetime extends AnalysisConfiguration {

	private final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace;
	private final NavigableMap<EventBasedTrace, TraceAggregationBuffer> trace2buffer;
	private final List<IPipe> tcpRelayPipes = new ArrayList<IPipe>();

	private final int numWorkerThreads;

	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	public TcpTraceReductionTeetime(final int numWorkerThreads) {
		this.numWorkerThreads = Math.min(numWorkerThreads, Common.NUM_VIRTUAL_CORES);
		this.traceId2trace = new ConcurrentHashMapWithDefault<Long, EventBasedTrace>(EventBasedTraceFactory.INSTANCE);
		this.trace2buffer = new TreeMap<EventBasedTrace, TraceAggregationBuffer>(new EventBasedTraceComperator());
		this.intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		this.interThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);

		init();
	}

	private void init() {
		Pipeline<Distributor<IMonitoringRecord>> tcpPipeline = Common.buildTcpPipeline(intraThreadPipeFactory);
		addThreadableStage(tcpPipeline);

		Pipeline<Distributor<Long>> clockStage = this.buildClockPipeline(5000);
		addThreadableStage(clockStage);

		for (int i = 0; i < this.numWorkerThreads; i++) {
			Stage pipeline = this.buildPipeline(tcpPipeline.getLastStage(), clockStage.getLastStage());
			addThreadableStage(pipeline);
		}
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		Clock clock = new Clock();
		clock.setInitialDelayInMs(intervalDelayInMs);
		clock.setIntervalDelayInMs(intervalDelayInMs);
		Distributor<Long> distributor = new Distributor<Long>();

		intraThreadPipeFactory.create(clock.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<Long>>(clock, distributor);
	}

	private Stage buildPipeline(final Distributor<IMonitoringRecord> tcpReaderPipeline, final Distributor<Long> clockStage) {
		// create stages
		Relay<IMonitoringRecord> relay = new Relay<IMonitoringRecord>();
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		EveryXthPrinter<EventBasedTrace> everyXthPrinter = new EveryXthPrinter<EventBasedTrace>(Common.NUM_ELEMENTS_LOG_TRIGGER);
		TraceReductionFilter traceReductionFilter = new TraceReductionFilter(this.trace2buffer);
		Sink<TraceAggregationBuffer> endStage = new Sink<TraceAggregationBuffer>();

		// connect stages
		IPipe tcpRelayPipe = interThreadPipeFactory.create(tcpReaderPipeline.getNewOutputPort(), relay.getInputPort(), Common.TCP_RELAY_MAX_SIZE);
		this.tcpRelayPipes.add(tcpRelayPipe);

		intraThreadPipeFactory.create(relay.getOutputPort(), instanceOfFilter.getInputPort());
		intraThreadPipeFactory.create(instanceOfFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		intraThreadPipeFactory.create(traceReconstructionFilter.getTraceValidOutputPort(), everyXthPrinter.getInputPort());
		intraThreadPipeFactory.create(everyXthPrinter.getNewOutputPort(), traceReductionFilter.getInputPort());
		intraThreadPipeFactory.create(traceReductionFilter.getOutputPort(), endStage.getInputPort());

		interThreadPipeFactory.create(clockStage.getNewOutputPort(), traceReductionFilter.getTriggerInputPort(), 10);

		return relay;
	}

	public void printNumWaits() {
		int maxNumWaits = 0;
		for (IPipe pipe : this.tcpRelayPipes) {
			SpScPipe interThreadPipe = (SpScPipe) pipe;
			// TODO introduce IInterThreadPipe
			maxNumWaits = Math.max(maxNumWaits, interThreadPipe.getNumWaits());
		}
		System.out.println("max #waits of TcpRelayPipes: " + maxNumWaits);
	}

	public int getNumWorkerThreads() {
		return this.numWorkerThreads;
	}

	public static void main(final String[] args) {
		int numWorkerThreads = Integer.valueOf(args[0]);

		final TcpTraceReductionTeetime configuration = new TcpTraceReductionTeetime(numWorkerThreads);

		Analysis analysis = new Analysis(configuration);
		analysis.init();

		Collection<Pair<Thread, Throwable>> exceptions = analysis.start();

		System.out.println("Exceptions: " + exceptions.size());
		configuration.printNumWaits();
	}

}
