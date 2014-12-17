package teetime.examples.kiekerdays;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import teetime.framework.Stage;
import teetime.framework.Pipeline;
import teetime.framework.RunnableProducerStage;
import teetime.framework.pipe.IPipe;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.framework.pipe.SpScPipe;
import teetime.stage.InstanceOfFilter;
import teetime.stage.Relay;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReader;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;
import teetime.util.concurrent.hashmap.TraceBuffer;

import kieker.analysis.plugin.filter.flow.TraceEventRecords;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;

public class TcpTraceReconstruction {

	private static final int NUM_VIRTUAL_CORES = Runtime.getRuntime().availableProcessors();
	private static final int MIO = 1000000;
	private static final int TCP_RELAY_MAX_SIZE = 2 * MIO;

	private final List<TraceEventRecords> elementCollection = new LinkedList<TraceEventRecords>();
	private final ConcurrentHashMapWithDefault<Long, TraceBuffer> traceId2trace = new ConcurrentHashMapWithDefault<Long, TraceBuffer>(new TraceBuffer());
	private final List<IPipe> tcpRelayPipes = new ArrayList<IPipe>();

	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	private Thread tcpThread;
	private Thread[] workerThreads;

	private int numWorkerThreads;

	public TcpTraceReconstruction() {
		intraThreadPipeFactory = PipeFactoryRegistry.INSTANCE.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		interThreadPipeFactory = PipeFactoryRegistry.INSTANCE.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);
	}

	public void init() {
		Pipeline<Distributor<IMonitoringRecord>> tcpPipeline = this.buildTcpPipeline();
		this.tcpThread = new Thread(new RunnableProducerStage(tcpPipeline));

		this.numWorkerThreads = Math.min(NUM_VIRTUAL_CORES, this.numWorkerThreads);
		this.workerThreads = new Thread[this.numWorkerThreads];

		for (int i = 0; i < this.workerThreads.length; i++) {
			Stage pipeline = this.buildPipeline(tcpPipeline.getLastStage());
			this.workerThreads[i] = new Thread(new RunnableProducerStage(pipeline));
		}
	}

	private Pipeline<Distributor<IMonitoringRecord>> buildTcpPipeline() {
		TcpReader tcpReader = new TcpReader();
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

	public void start() {

		this.tcpThread.start();

		for (Thread workerThread : this.workerThreads) {
			workerThread.start();
		}

		try {
			this.tcpThread.join();

			for (Thread workerThread : this.workerThreads) {
				workerThread.join();
			}
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}

	public void onTerminate() {
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

	public void setNumWorkerThreads(final int numWorkerThreads) {
		this.numWorkerThreads = numWorkerThreads;
	}

	public static void main(final String[] args) {
		int numWorkerThreads = Integer.valueOf(args[0]);

		final TcpTraceReconstruction analysis = new TcpTraceReconstruction();
		analysis.setNumWorkerThreads(numWorkerThreads);

		analysis.init();
		try {
			analysis.start();
		} finally {
			analysis.onTerminate();
		}
	}

}
