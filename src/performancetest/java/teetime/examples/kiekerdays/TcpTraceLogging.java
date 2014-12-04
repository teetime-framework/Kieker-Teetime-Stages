package teetime.examples.kiekerdays;

import teetime.framework.IStage;
import teetime.framework.RunnableStage;
import teetime.stage.io.network.TcpReader;

public class TcpTraceLogging {

	private Thread tcpThread;

	public void init() {
		IStage tcpPipeline = this.buildTcpPipeline();
		this.tcpThread = new Thread(new RunnableStage(tcpPipeline));
	}

	public void start() {

		this.tcpThread.start();

		try {
			this.tcpThread.join();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}

	private IStage buildTcpPipeline() {
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
