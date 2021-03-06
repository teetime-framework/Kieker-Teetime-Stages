/**
 * Copyright (C) 2015 Christian Wulf, Nelson Tavares de Sousa (http://teetime-framework.github.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package teetime.examples.traceReading;

import java.util.List;

import kieker.common.record.IMonitoringRecord;

import teetime.framework.AbstractStage;
import teetime.framework.Configuration;
import teetime.framework.Pipeline;
import teetime.stage.Clock;
import teetime.stage.Counter;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReaderStage;

public class TcpTraceLoggingExtAnalysisConfiguration extends Configuration {

	private Counter<IMonitoringRecord> recordCounter;
	private ElementThroughputMeasuringStage<IMonitoringRecord> recordThroughputStage;

	public TcpTraceLoggingExtAnalysisConfiguration() {
		init();
	}

	private void init() {
		final Pipeline<Distributor<Long>> clockPipeline = this.buildClockPipeline(1000);
		this.buildTcpPipeline(clockPipeline.getLastStage());
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		Clock clockStage = new Clock();
		clockStage.setInitialDelayInMs(intervalDelayInMs);
		clockStage.setIntervalDelayInMs(intervalDelayInMs);
		Distributor<Long> distributor = new Distributor<Long>();

		connectPorts(clockStage.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<Long>>(clockStage, distributor);
	}

	private AbstractStage buildTcpPipeline(final Distributor<Long> previousClockStage) {
		TcpReaderStage tcpReader = new TcpReaderStage();
		this.recordCounter = new Counter<IMonitoringRecord>();
		this.recordThroughputStage = new ElementThroughputMeasuringStage<IMonitoringRecord>();
		Sink<IMonitoringRecord> endStage = new Sink<IMonitoringRecord>();

		connectPorts(tcpReader.getOutputPort(), this.recordCounter.getInputPort());
		connectPorts(this.recordCounter.getOutputPort(), this.recordThroughputStage.getInputPort());
		connectPorts(this.recordThroughputStage.getOutputPort(), endStage.getInputPort());
		// intraThreadPipeFactory.create(this.recordCounter.getOutputPort(), endStage.getInputPort());

		connectPorts(previousClockStage.getNewOutputPort(), this.recordThroughputStage.getTriggerInputPort(), 10);

		recordThroughputStage.declareActive();

		return tcpReader;
	}

	public int getNumRecords() {
		return this.recordCounter.getNumElementsPassed();
	}

	public List<Long> getRecordThroughputs() {
		return this.recordThroughputStage.getThroughputs();
	}

}
