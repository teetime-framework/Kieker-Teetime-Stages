package experiment.fse15.teetime;

import java.util.Collection;

import teetime.framework.Analysis;
import teetime.framework.AnalysisConfiguration;
import teetime.framework.Pipeline;
import teetime.framework.pipe.IPipeFactory;
import teetime.framework.pipe.PipeFactoryRegistry.PipeOrdering;
import teetime.framework.pipe.PipeFactoryRegistry.ThreadCommunication;
import teetime.stage.basic.distributor.Distributor;
import teetime.util.Pair;

import kieker.common.record.IMonitoringRecord;

class TcpTraceLoggingTeetime extends AnalysisConfiguration {

	private final IPipeFactory intraThreadPipeFactory;

	public TcpTraceLoggingTeetime() {
		intraThreadPipeFactory = PIPE_FACTORY_REGISTRY.getPipeFactory(ThreadCommunication.INTRA, PipeOrdering.ARBITRARY, false);
		init();
	}

	private void init() {
		Pipeline<Distributor<IMonitoringRecord>> tcpPipeline = Common.buildTcpPipeline(intraThreadPipeFactory);
		addThreadableStage(tcpPipeline);

		tcpPipeline.getLastStage().getNewOutputPort();
	}

	// private Stage buildTcpPipeline() {
	// // TCPReaderSink tcpReader = new TCPReaderSink();
	// TcpReaderStage tcpReader = new TcpReaderStage();
	//
	// return tcpReader;
	// }

	public static void main(final String[] args) {
		final TcpTraceLoggingTeetime configuration = new TcpTraceLoggingTeetime();

		Analysis analysis = new Analysis(configuration);
		analysis.init();
		Collection<Pair<Thread, Throwable>> exceptions = analysis.start();

		System.out.println("Exceptions: " + exceptions.size());
	}

}
