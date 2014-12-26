package teetime.examples.kiekerdays;

import teetime.framework.AnalysisConfiguration;
import teetime.framework.RunnableProducerStage;
import teetime.framework.Stage;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.stage.basic.Sink;
import teetime.stage.explorviz.KiekerRecordTcpReader;

import kieker.common.record.IMonitoringRecord;

class TcpTraceLoggingExplorviz extends AnalysisConfiguration {

	private Thread tcpThread;
	private final IPipeFactory intraThreadPipeFactory;

	public TcpTraceLoggingExplorviz() {
		intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		// interThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTER, PipeOrdering.QUEUE_BASED, false);
		init();
	}

	private void init() {
		Stage tcpPipeline = this.buildTcpPipeline();
		this.tcpThread = new Thread(new RunnableProducerStage(tcpPipeline));
	}

	public void start() {
		this.tcpThread.start();

		try {
			this.tcpThread.join();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}

	private Stage buildTcpPipeline() {
		KiekerRecordTcpReader tcpReader = new KiekerRecordTcpReader();
		Sink<IMonitoringRecord> endStage = new Sink<IMonitoringRecord>();

		intraThreadPipeFactory.create(tcpReader.getOutputPort(), endStage.getInputPort());

		return tcpReader;
	}

	public static void main(final String[] args) {
		final TcpTraceLoggingExplorviz analysis = new TcpTraceLoggingExplorviz();

		analysis.init();
		try {
			analysis.start();
		} finally {
		}
	}

}
