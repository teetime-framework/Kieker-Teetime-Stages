package experiment.fse15.teetime;

import java.util.ArrayList;
import java.util.Collection;
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
import teetime.stage.Relay;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.util.Pair;

import kieker.common.record.IMonitoringRecord;

class TcpTraceThreadedLoggingTeetime extends AnalysisConfiguration {

	private final int numWorkerThreads;
	private final IPipeFactory intraThreadPipeFactory;
	private final IPipeFactory interThreadPipeFactory;

	private final List<IPipe> tcpRelayPipes = new ArrayList<IPipe>();

	public TcpTraceThreadedLoggingTeetime(final int numWorkerThreads) {
		this.numWorkerThreads = Math.min(numWorkerThreads, Common.NUM_VIRTUAL_CORES);

		this.intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		this.interThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);

		init();
	}

	private void init() {
		Pipeline<Distributor<IMonitoringRecord>> tcpPipeline = Common.buildTcpPipeline(intraThreadPipeFactory);
		addThreadableStage(tcpPipeline);

		for (int i = 0; i < this.numWorkerThreads; i++) {
			Stage pipeline = this.buildPipeline(tcpPipeline.getLastStage());
			addThreadableStage(pipeline);
		}
	}

	private Stage buildPipeline(final Distributor<IMonitoringRecord> tcpReaderPipeline) {
		// create stages
		Relay<IMonitoringRecord> relay = new Relay<IMonitoringRecord>();
		Sink<IMonitoringRecord> endStage = new Sink<IMonitoringRecord>();

		// connect stages
		IPipe tcpRelayPipe = interThreadPipeFactory.create(tcpReaderPipeline.getNewOutputPort(), relay.getInputPort(), Common.TCP_RELAY_MAX_SIZE);
		this.tcpRelayPipes.add(tcpRelayPipe);

		intraThreadPipeFactory.create(relay.getOutputPort(), endStage.getInputPort());

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

	public int getNumWorkerThreads() {
		return this.numWorkerThreads;
	}

	public static void main(final String[] args) {
		int numWorkerThreads = Integer.valueOf(args[0]);

		final TcpTraceThreadedLoggingTeetime configuration = new TcpTraceThreadedLoggingTeetime(numWorkerThreads);

		Analysis analysis = new Analysis(configuration);
		analysis.init();
		Collection<Pair<Thread, Throwable>> exceptions = analysis.start();

		System.out.println("Exceptions: " + exceptions.size());
		configuration.printNumWaits();
	}

}
