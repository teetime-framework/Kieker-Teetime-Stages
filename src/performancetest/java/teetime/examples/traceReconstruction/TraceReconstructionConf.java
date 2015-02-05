package teetime.examples.traceReconstruction;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import teetime.framework.AnalysisConfiguration;
import teetime.framework.Stage;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.stage.Cache;
import teetime.stage.Clock;
import teetime.stage.CollectorSink;
import teetime.stage.Counter;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.InitialElementProducer;
import teetime.stage.InstanceOfFilter;
import teetime.stage.basic.merger.Merger;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.io.filesystem.Dir2RecordsFilter;
import teetime.stage.string.buffer.StringBufferFilter;
import teetime.stage.string.buffer.handler.MonitoringRecordHandler;
import teetime.stage.string.buffer.handler.StringHandler;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;
import teetime.util.concurrent.hashmap.TraceBufferList;

import kieker.analysis.plugin.filter.flow.TraceEventRecords;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;

public class TraceReconstructionConf extends AnalysisConfiguration {

	private final List<TraceEventRecords> elementCollection = new LinkedList<TraceEventRecords>();

	private final File inputDir;
	private final ConcurrentHashMapWithDefault<Long, TraceBufferList> traceId2trace;
	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	private ClassNameRegistryRepository classNameRegistryRepository;
	private Counter<IMonitoringRecord> recordCounter;
	private Counter<TraceEventRecords> traceCounter;
	private ElementThroughputMeasuringStage<IFlowRecord> throughputFilter;

	public TraceReconstructionConf(final File inputDir) {
		this.inputDir = inputDir;
		traceId2trace = new ConcurrentHashMapWithDefault<Long, TraceBufferList>(new TraceBufferList());
		intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		interThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);
		init();
	}

	private void init() {
		Clock clockStage = this.buildClockPipeline();
		addThreadableStage(clockStage);

		Stage pipeline = this.buildPipeline(clockStage);
		addThreadableStage(pipeline);
	}

	private Clock buildClockPipeline() {
		Clock clock = new Clock();
		clock.setIntervalDelayInMs(100);

		return clock;
	}

	private Stage buildPipeline(final Clock clockStage) {
		this.classNameRegistryRepository = new ClassNameRegistryRepository();

		// create stages
		InitialElementProducer<File> initialElementProducer = new InitialElementProducer<File>(this.inputDir);
		final Dir2RecordsFilter dir2RecordsFilter = new Dir2RecordsFilter(this.classNameRegistryRepository);
		this.recordCounter = new Counter<IMonitoringRecord>();
		final Cache<IMonitoringRecord> cache = new Cache<IMonitoringRecord>();

		final StringBufferFilter<IMonitoringRecord> stringBufferFilter = new StringBufferFilter<IMonitoringRecord>();
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		this.throughputFilter = new ElementThroughputMeasuringStage<IFlowRecord>();
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		Merger<TraceEventRecords> merger = new Merger<TraceEventRecords>();
		this.traceCounter = new Counter<TraceEventRecords>();
		final CollectorSink<TraceEventRecords> collector = new CollectorSink<TraceEventRecords>(this.elementCollection);

		// configure stages
		stringBufferFilter.getDataTypeHandlers().add(new MonitoringRecordHandler());
		stringBufferFilter.getDataTypeHandlers().add(new StringHandler());

		// connect stages
		intraThreadPipeFactory.create(initialElementProducer.getOutputPort(), dir2RecordsFilter.getInputPort());
		intraThreadPipeFactory.create(dir2RecordsFilter.getOutputPort(), this.recordCounter.getInputPort());
		intraThreadPipeFactory.create(this.recordCounter.getOutputPort(), cache.getInputPort());
		intraThreadPipeFactory.create(cache.getOutputPort(), stringBufferFilter.getInputPort());
		intraThreadPipeFactory.create(stringBufferFilter.getOutputPort(), instanceOfFilter.getInputPort());
		intraThreadPipeFactory.create(instanceOfFilter.getOutputPort(), this.throughputFilter.getInputPort());
		intraThreadPipeFactory.create(this.throughputFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		// intraThreadPipeFactory.create(instanceOfFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		intraThreadPipeFactory.create(traceReconstructionFilter.getTraceValidOutputPort(), merger.getNewInputPort());
		intraThreadPipeFactory.create(traceReconstructionFilter.getTraceInvalidOutputPort(), merger.getNewInputPort());
		intraThreadPipeFactory.create(merger.getOutputPort(), this.traceCounter.getInputPort());
		intraThreadPipeFactory.create(this.traceCounter.getOutputPort(), collector.getInputPort());

		interThreadPipeFactory.create(clockStage.getOutputPort(), this.throughputFilter.getTriggerInputPort(), 1);

		return initialElementProducer;
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

	public List<Long> getThroughputs() {
		return this.throughputFilter.getThroughputs();
	}

	public File getInputDir() {
		return this.inputDir;
	}

}
