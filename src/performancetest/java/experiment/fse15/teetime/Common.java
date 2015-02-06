package experiment.fse15.teetime;

import teetime.framework.Pipeline;
import teetime.framework.pipe.IPipeFactory;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReaderStage;

import kieker.common.record.IMonitoringRecord;

final class Common {

	static final int NUM_ELEMENTS = 1000000;

	private Common() {
		// utility class
	}

	public static Pipeline<Distributor<IMonitoringRecord>> buildTcpPipeline(final IPipeFactory intraThreadPipeFactory) {
		TcpReaderStage tcpReader = new TcpReaderStage();
		Distributor<IMonitoringRecord> distributor = new Distributor<IMonitoringRecord>();

		intraThreadPipeFactory.create(tcpReader.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<IMonitoringRecord>>(tcpReader, distributor);
	}

}
