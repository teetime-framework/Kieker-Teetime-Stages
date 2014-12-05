package teetime.examples.traceReductionWithThreads;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import teetime.framework.AbstractStage;
import teetime.framework.AnalysisConfiguration;
import teetime.framework.IStage;
import teetime.framework.pipe.IPipe;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.framework.pipe.SpScPipe;
import teetime.stage.Clock;
import teetime.stage.Counter;
import teetime.stage.ElementDelayMeasuringStage;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.InstanceCounter;
import teetime.stage.InstanceOfFilter;
import teetime.stage.Pipeline;
import teetime.stage.Relay;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReader;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.stage.trace.traceReduction.TraceAggregationBuffer;
import teetime.stage.trace.traceReduction.TraceComperator;
import teetime.stage.trace.traceReduction.TraceReductionFilter;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;
import teetime.util.concurrent.hashmap.TraceBuffer;

import kieker.analysis.plugin.filter.flow.TraceEventRecords;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;
import kieker.common.record.flow.trace.TraceMetadata;

public class TcpTraceReductionAnalysisWithThreadsConfiguration extends AnalysisConfiguration {

	private static final int NUM_VIRTUAL_CORES = Runtime.getRuntime().availableProcessors();
	private static final int MIO = 1000000;
	private static final int TCP_RELAY_MAX_SIZE = (int) (0.5 * MIO);

	private final List<TraceEventRecords> elementCollection = new LinkedList<TraceEventRecords>();

	private final List<IPipe> tcpRelayPipes = new ArrayList<IPipe>();
	private final int numWorkerThreads;
	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public TcpTraceReductionAnalysisWithThreadsConfiguration(final int numWorkerThreads) {
		this.numWorkerThreads = Math.min(NUM_VIRTUAL_CORES, numWorkerThreads);

		try {
			this.recordCounterFactory = new StageFactory(Counter.class.getConstructor());
			this.recordThroughputFilterFactory = new StageFactory(ElementDelayMeasuringStage.class.getConstructor());
			this.traceMetadataCounterFactory = new StageFactory(InstanceCounter.class.getConstructor(Class.class));
			this.traceCounterFactory = new StageFactory(Counter.class.getConstructor());
			this.traceThroughputFilterFactory = new StageFactory(ElementThroughputMeasuringStage.class.getConstructor());
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e);
		}

		intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		interThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);
		init();
	}

	private void init() {
		final Pipeline<Distributor<IMonitoringRecord>> tcpPipeline = this.buildTcpPipeline();
		addThreadableStage(tcpPipeline);

		final Pipeline<Distributor<Long>> clockStage = this.buildClockPipeline(1000);
		addThreadableStage(clockStage);

		final Pipeline<Distributor<Long>> clock2Stage = this.buildClockPipeline(5000);
		addThreadableStage(clock2Stage);

		for (int i = 0; i < this.numWorkerThreads; i++) {
			final IStage pipeline = this.buildPipeline(tcpPipeline, clockStage, clock2Stage);
			addThreadableStage(pipeline);
		}
	}

	private Pipeline<Distributor<IMonitoringRecord>> buildTcpPipeline() {
		final TcpReader tcpReader = new TcpReader();
		final Distributor<IMonitoringRecord> distributor = new Distributor<IMonitoringRecord>();

		intraThreadPipeFactory.create(tcpReader.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<IMonitoringRecord>>(tcpReader, distributor);
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		final Clock clock = new Clock();
		clock.setInitialDelayInMs(intervalDelayInMs);
		clock.setIntervalDelayInMs(intervalDelayInMs);
		final Distributor<Long> distributor = new Distributor<Long>();

		intraThreadPipeFactory.create(clock.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<Long>>(clock, distributor);
	}

	private static class StageFactory<T extends AbstractStage> {

		private final Constructor<T> constructor;
		private final List<T> stages = new ArrayList<T>();

		public StageFactory(final Constructor<T> constructor) {
			this.constructor = constructor;
		}

		public T create(final Object... initargs) {
			try {
				T stage = this.constructor.newInstance(initargs);
				this.stages.add(stage);
				return stage;
			} catch (InstantiationException e) {
				throw new IllegalStateException(e);
			} catch (IllegalAccessException e) {
				throw new IllegalStateException(e);
			} catch (IllegalArgumentException e) {
				throw new IllegalStateException(e);
			} catch (InvocationTargetException e) {
				throw new IllegalStateException(e);
			}
		}

		public List<T> getStages() {
			return this.stages;
		}
	}

	private final ConcurrentHashMapWithDefault<Long, TraceBuffer> traceId2trace = new ConcurrentHashMapWithDefault<Long, TraceBuffer>(new TraceBuffer());
	private final Map<TraceEventRecords, TraceAggregationBuffer> trace2buffer = new TreeMap<TraceEventRecords, TraceAggregationBuffer>(new TraceComperator());

	private final StageFactory<Counter<IMonitoringRecord>> recordCounterFactory;
	private final StageFactory<ElementDelayMeasuringStage<IMonitoringRecord>> recordThroughputFilterFactory;
	private final StageFactory<InstanceCounter<IMonitoringRecord, TraceMetadata>> traceMetadataCounterFactory;
	private final StageFactory<Counter<TraceEventRecords>> traceCounterFactory;
	private final StageFactory<ElementThroughputMeasuringStage<TraceEventRecords>> traceThroughputFilterFactory;

	private IStage buildPipeline(final Pipeline<Distributor<IMonitoringRecord>> tcpPipeline, final Pipeline<Distributor<Long>> clockStage,
			final Pipeline<Distributor<Long>> clock2Stage) {
		// create stages
		Relay<IMonitoringRecord> relay = new Relay<IMonitoringRecord>();
		Counter<IMonitoringRecord> recordCounter = this.recordCounterFactory.create();
		InstanceCounter<IMonitoringRecord, TraceMetadata> traceMetadataCounter = this.traceMetadataCounterFactory.create(TraceMetadata.class);
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		// ElementDelayMeasuringStage<IMonitoringRecord> recordThroughputFilter = this.recordThroughputFilterFactory.create();
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		TraceReductionFilter traceReductionFilter = new TraceReductionFilter(this.trace2buffer);
		Counter<TraceEventRecords> traceCounter = this.traceCounterFactory.create();
		ElementThroughputMeasuringStage<TraceEventRecords> traceThroughputFilter = this.traceThroughputFilterFactory.create();
		Sink<TraceEventRecords> endStage = new Sink<TraceEventRecords>();

		// connect stages
		final IPipe pipe = interThreadPipeFactory.create(tcpPipeline.getLastStage().getNewOutputPort(), relay.getInputPort(), TCP_RELAY_MAX_SIZE);
		this.tcpRelayPipes.add(pipe);

		intraThreadPipeFactory.create(relay.getOutputPort(), recordCounter.getInputPort());
		intraThreadPipeFactory.create(recordCounter.getOutputPort(), traceMetadataCounter.getInputPort());
		intraThreadPipeFactory.create(traceMetadataCounter.getOutputPort(), instanceOfFilter.getInputPort());
		intraThreadPipeFactory.create(instanceOfFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		intraThreadPipeFactory.create(traceReconstructionFilter.getTraceValidOutputPort(), traceReductionFilter.getInputPort());
		intraThreadPipeFactory.create(traceReductionFilter.getOutputPort(), traceCounter.getInputPort());
		intraThreadPipeFactory.create(traceCounter.getOutputPort(), traceThroughputFilter.getInputPort());
		intraThreadPipeFactory.create(traceThroughputFilter.getOutputPort(), endStage.getInputPort());

		// intraThreadPipeFactory.create(traceReconstructionFilter.getOutputPort(), traceThroughputFilter.getInputPort());
		// intraThreadPipeFactory.create(traceThroughputFilter.getOutputPort(), endStage.getInputPort());

		interThreadPipeFactory.create(clock2Stage.getLastStage().getNewOutputPort(), traceReductionFilter.getTriggerInputPort(), 10);
		interThreadPipeFactory.create(clockStage.getLastStage().getNewOutputPort(), traceThroughputFilter.getTriggerInputPort(), 10);

		return relay;
	}

	public List<TraceEventRecords> getElementCollection() {
		return this.elementCollection;
	}

	public int getNumRecords() {
		int sum = 0;
		for (Counter<IMonitoringRecord> stage : this.recordCounterFactory.getStages()) {
			sum += stage.getNumElementsPassed();
		}
		return sum;
	}

	public int getNumTraces() {
		int sum = 0;
		for (Counter<TraceEventRecords> stage : this.traceCounterFactory.getStages()) {
			sum += stage.getNumElementsPassed();
		}
		return sum;
	}

	public List<Long> getRecordDelays() {
		List<Long> throughputs = new LinkedList<Long>();
		for (ElementDelayMeasuringStage<IMonitoringRecord> stage : this.recordThroughputFilterFactory.getStages()) {
			throughputs.addAll(stage.getDelays());
		}
		return throughputs;
	}

	public List<Long> getTraceThroughputs() {
		List<Long> throughputs = new LinkedList<Long>();
		for (ElementThroughputMeasuringStage<TraceEventRecords> stage : this.traceThroughputFilterFactory.getStages()) {
			throughputs.addAll(stage.getThroughputs());
		}
		return throughputs;
	}

	public int getMaxNumWaits() {
		int maxNumWaits = 0;
		for (IPipe pipe : this.tcpRelayPipes) {
			SpScPipe interThreadPipe = (SpScPipe) pipe;
			maxNumWaits = Math.max(maxNumWaits, interThreadPipe.getNumWaits());
		}
		return maxNumWaits;
	}

	public int getNumWorkerThreads() {
		return this.numWorkerThreads;
	}

	public List<Integer> getNumTraceMetadatas() {
		List<Integer> numTraceMetadatas = new LinkedList<Integer>();
		for (InstanceCounter<IMonitoringRecord, TraceMetadata> stage : this.traceMetadataCounterFactory.getStages()) {
			numTraceMetadatas.add(stage.getCounter());
		}
		return numTraceMetadatas;
	}

}
