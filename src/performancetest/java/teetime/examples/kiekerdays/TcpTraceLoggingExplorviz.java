package teetime.examples.kiekerdays;

import teetime.framework.IStage;
import teetime.framework.RunnableStage;
import teetime.framework.pipe.SingleElementPipe;
import teetime.stage.basic.Sink;
import teetime.stage.explorviz.KiekerRecordTcpReader;

import kieker.common.record.IMonitoringRecord;

public class TcpTraceLoggingExplorviz {

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
		KiekerRecordTcpReader tcpReader = new KiekerRecordTcpReader();
		Sink<IMonitoringRecord> endStage = new Sink<IMonitoringRecord>();

		SingleElementPipe.connect(tcpReader.getOutputPort(), endStage.getInputPort());

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
