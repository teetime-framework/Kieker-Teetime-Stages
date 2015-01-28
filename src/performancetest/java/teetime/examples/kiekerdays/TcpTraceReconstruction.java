package teetime.examples.kiekerdays;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import teetime.framework.Analysis;
import teetime.framework.AnalysisConfiguration;
import teetime.framework.Pipeline;
import teetime.framework.Stage;
import teetime.framework.pipe.IPipe;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.framework.pipe.SpScPipe;
import teetime.stage.InstanceOfFilter;
import teetime.stage.Relay;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReaderStage;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.util.Pair;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;
import teetime.util.concurrent.hashmap.TraceBuffer;

import kieker.analysis.plugin.filter.flow.TraceEventRecords;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;

class TcpTraceReconstruction extends AnalysisConfiguration {

	private static final int NUM_VIRTUAL_CORES = Runtime.getRuntime().availableProcessors();
	private static final int MIO = 1000000;
	private static final int TCP_RELAY_MAX_SIZE = 2 * MIO;

	private final List<TraceEventRecords> elementCollection = new LinkedList<TraceEventRecords>();
	private final ConcurrentHashMapWithDefault<Long, TraceBuffer> traceId2trace = new ConcurrentHashMapWithDefault<Long, TraceBuffer>(new TraceBuffer());
	private final List<IPipe> tcpRelayPipes = new ArrayList<IPipe>();

	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	private final int numWorkerThreads;

	public TcpTraceReconstruction(final int numWorkerThreads) {
		intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		interThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);
		this.numWorkerThreads = Math.min(NUM_VIRTUAL_CORES, numWorkerThreads);
		init();
	}

	private void init() {
		Pipeline<Distributor<IMonitoringRecord>> tcpPipeline = this.buildTcpPipeline();
		addThreadableStage(tcpPipeline);

		for (int i = 0; i < this.numWorkerThreads; i++) {
			Stage pipeline = this.buildPipeline(tcpPipeline.getLastStage());
			addThreadableStage(pipeline);
		}
	}

	private Pipeline<Distributor<IMonitoringRecord>> buildTcpPipeline() {
		TcpReaderStage tcpReader = new TcpReaderStage();
		Distributor<IMonitoringRecord> distributor = new Distributor<IMonitoringRecord>();

		intraThreadPipeFactory.create(tcpReader.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<IMonitoringRecord>>(tcpReader, distributor);
	}

	private Stage buildPipeline(final Distributor<IMonitoringRecord> tcpReaderPipeline) {
		// create stages
		Relay<IMonitoringRecord> relay = new Relay<IMonitoringRecord>();
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		Sink<TraceEventRecords> endStage = new Sink<TraceEventRecords>();

		// connect stages
		IPipe tcpRelayPipe = interThreadPipeFactory.create(tcpReaderPipeline.getNewOutputPort(), relay.getInputPort(), TCP_RELAY_MAX_SIZE);
		this.tcpRelayPipes.add(tcpRelayPipe);

		intraThreadPipeFactory.create(relay.getOutputPort(), instanceOfFilter.getInputPort());
		intraThreadPipeFactory.create(instanceOfFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		intraThreadPipeFactory.create(traceReconstructionFilter.getTraceValidOutputPort(), endStage.getInputPort());

		return relay;
	}

	public void printNumWaits() {
		int maxNumWaits = 0;
		for (IPipe pipe : this.tcpRelayPipes) {
			SpScPipe interThreadPipe = (SpScPipe) pipe;
			maxNumWaits = Math.max(maxNumWaits, interThreadPipe.getNumWaits());
		}
		System.out.println("max #waits of TcpRelayPipes: " + maxNumWaits);
	}

	public List<TraceEventRecords> getElementCollection() {
		return this.elementCollection;
	}

	public int getNumWorkerThreads() {
		return this.numWorkerThreads;
	}

	public static void main(final String[] args) {
		int numWorkerThreads = Integer.valueOf(args[0]);

		final TcpTraceReconstruction configuration = new TcpTraceReconstruction(numWorkerThreads);

		Analysis analysis = new Analysis(configuration);
		analysis.init();
		Collection<Pair<Thread, Throwable>> exceptions = analysis.start();

		System.out.println("Exceptions: " + exceptions.size());
		configuration.printNumWaits();
	}

}
