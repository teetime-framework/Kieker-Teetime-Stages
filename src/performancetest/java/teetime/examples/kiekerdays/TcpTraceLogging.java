package teetime.examples.kiekerdays;

import teetime.framework.Stage;
import teetime.framework.RunnableProducerStage;
import teetime.stage.io.network.TcpReader;

public class TcpTraceLogging {

	private Thread tcpThread;

	public void init() {
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
		// TCPReaderSink tcpReader = new TCPReaderSink();
		TcpReader tcpReader = new TcpReader();

		return tcpReader;
	}

	public static void main(final String[] args) {
		final TcpTraceLogging analysis = new TcpTraceLogging();

		analysis.init();
		try {
			analysis.start();
		} finally {
		}
	}

}
