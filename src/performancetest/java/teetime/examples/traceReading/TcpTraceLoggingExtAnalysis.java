package teetime.examples.traceReading;

import java.util.List;

import teetime.framework.OldHeadPipeline;
import teetime.framework.RunnableStage;
import teetime.framework.pipe.SingleElementPipe;
import teetime.framework.pipe.SpScPipe;
import teetime.stage.Clock;
import teetime.stage.Counter;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReader;

import kieker.common.record.IMonitoringRecord;

public class TcpTraceLoggingExtAnalysis {

	private Thread clockThread;
	private Thread tcpThread;

	private Counter<IMonitoringRecord> recordCounter;
	private ElementThroughputMeasuringStage<IMonitoringRecord> recordThroughputStage;

	private OldHeadPipeline<Clock, Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		Clock clockStage = new Clock();
		clockStage.setInitialDelayInMs(intervalDelayInMs);
		clockStage.setIntervalDelayInMs(intervalDelayInMs);
		Distributor<Long> distributor = new Distributor<Long>();

		SingleElementPipe.connect(clockStage.getOutputPort(), distributor.getInputPort());

		// create and configure pipeline
		OldHeadPipeline<Clock, Distributor<Long>> pipeline = new OldHeadPipeline<Clock, Distributor<Long>>();
		pipeline.setFirstStage(clockStage);
		pipeline.setLastStage(distributor);
		return pipeline;
	}

	private OldHeadPipeline<?, ?> buildTcpPipeline(final Distributor<Long> previousClockStage) {
		TcpReader tcpReader = new TcpReader();
		this.recordCounter = new Counter<IMonitoringRecord>();
		this.recordThroughputStage = new ElementThroughputMeasuringStage<IMonitoringRecord>();
		Sink<IMonitoringRecord> endStage = new Sink<IMonitoringRecord>();

		SingleElementPipe.connect(tcpReader.getOutputPort(), this.recordCounter.getInputPort());
		SingleElementPipe.connect(this.recordCounter.getOutputPort(), this.recordThroughputStage.getInputPort());
		SingleElementPipe.connect(this.recordThroughputStage.getOutputPort(), endStage.getInputPort());
		// SingleElementPipe.connect(this.recordCounter.getOutputPort(), endStage.getInputPort());

		SpScPipe.connect(previousClockStage.getNewOutputPort(), this.recordThroughputStage.getTriggerInputPort(), 10);

		// create and configure pipeline
		OldHeadPipeline<TcpReader, Sink<IMonitoringRecord>> pipeline = new OldHeadPipeline<TcpReader, Sink<IMonitoringRecord>>();
		pipeline.setFirstStage(tcpReader);
		pipeline.setLastStage(endStage);
		return pipeline;
	}

	public void init() {

		OldHeadPipeline<Clock, Distributor<Long>> clockPipeline = this.buildClockPipeline(1000);
		this.clockThread = new Thread(new RunnableStage(clockPipeline));

		OldHeadPipeline<?, ?> tcpPipeline = this.buildTcpPipeline(clockPipeline.getLastStage());
		this.tcpThread = new Thread(new RunnableStage(tcpPipeline));
	}

	public void start() {

		this.tcpThread.start();
		this.clockThread.start();

		try {
			this.tcpThread.join();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}

		this.clockThread.interrupt();
	}

	public int getNumRecords() {
		return this.recordCounter.getNumElementsPassed();
	}

	public List<Long> getRecordThroughputs() {
		return this.recordThroughputStage.getThroughputs();
	}

}
