package teetime.examples.kiekerdays;

import java.util.Collection;

import teetime.framework.Analysis;
import teetime.framework.AnalysisConfiguration;
import teetime.framework.Stage;
import teetime.stage.io.network.TcpReader;
import teetime.util.Pair;

class TcpTraceLoggingConfiguration extends AnalysisConfiguration {

	public TcpTraceLoggingConfiguration() {
		Stage tcpPipeline = this.buildTcpPipeline();
		addThreadableStage(tcpPipeline);
	}

	private Stage buildTcpPipeline() {
		// TCPReaderSink tcpReader = new TCPReaderSink();
		TcpReader tcpReader = new TcpReader();

		return tcpReader;
	}

	public static void main(final String[] args) {
		final TcpTraceLoggingConfiguration configuration = new TcpTraceLoggingConfiguration();

		Analysis analysis = new Analysis(configuration);
		analysis.init();
		Collection<Pair<Thread, Throwable>> exceptions = analysis.start();

		if (!exceptions.isEmpty()) {
			throw new IllegalStateException();
		}
	}

}
