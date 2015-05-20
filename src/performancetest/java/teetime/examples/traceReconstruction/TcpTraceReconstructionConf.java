/**
 * Copyright (C) 2015 Christian Wulf, Nelson Tavares de Sousa (http://teetime.sourceforge.net)
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
package teetime.examples.traceReconstruction;

import java.util.List;

import teetime.framework.AnalysisConfiguration;
import teetime.framework.Pipeline;
import teetime.framework.Stage;
import teetime.stage.Clock;
import teetime.stage.Counter;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.InstanceOfFilter;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReaderStage;
import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.stage.trace.traceReconstruction.EventBasedTraceFactory;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.util.ConcurrentHashMapWithDefault;

import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;

public class TcpTraceReconstructionConf extends AnalysisConfiguration {

	private static final int MIO = 1000000;
	private static final int TCP_RELAY_MAX_SIZE = 2 * MIO;

	private final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace;

	private Counter<IMonitoringRecord> recordCounter;
	private Counter<EventBasedTrace> traceCounter;
	private ElementThroughputMeasuringStage<IFlowRecord> recordThroughputFilter;
	private ElementThroughputMeasuringStage<EventBasedTrace> traceThroughputFilter;

	public TcpTraceReconstructionConf() {
		this.traceId2trace = new ConcurrentHashMapWithDefault<Long, EventBasedTrace>(EventBasedTraceFactory.INSTANCE);
		init();
	}

	private void init() {
		Pipeline<Distributor<Long>> clockStage = this.buildClockPipeline(1000);
		addThreadableStage(clockStage);

		Pipeline<Distributor<Long>> clock2Stage = this.buildClockPipeline(2000);
		addThreadableStage(clock2Stage);

		Stage pipeline = this.buildPipeline(clockStage.getLastStage(), clock2Stage.getLastStage());
		addThreadableStage(pipeline);
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		Clock clock = new Clock();
		clock.setIntervalDelayInMs(intervalDelayInMs);
		Distributor<Long> distributor = new Distributor<Long>();

		connectIntraThreads(clock.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<Long>>(clock, distributor);
	}

	private Stage buildPipeline(final Distributor<Long> clockStage, final Distributor<Long> clock2Stage) {
		// create stages
		TcpReaderStage tcpReader = new TcpReaderStage();
		this.recordCounter = new Counter<IMonitoringRecord>();
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		this.recordThroughputFilter = new ElementThroughputMeasuringStage<IFlowRecord>();
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		this.traceThroughputFilter = new ElementThroughputMeasuringStage<EventBasedTrace>();
		this.traceCounter = new Counter<EventBasedTrace>();
		Sink<EventBasedTrace> endStage = new Sink<EventBasedTrace>();

		// connect stages
		connectBoundedInterThreads(tcpReader.getOutputPort(), this.recordCounter.getInputPort(), TCP_RELAY_MAX_SIZE);
		connectIntraThreads(this.recordCounter.getOutputPort(), instanceOfFilter.getInputPort());
		// connectIntraThreads(instanceOfFilter.getOutputPort(), this.recordThroughputFilter.getInputPort());
		// connectIntraThreads(this.recordThroughputFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		connectIntraThreads(instanceOfFilter.getMatchedOutputPort(), traceReconstructionFilter.getInputPort());
		connectIntraThreads(traceReconstructionFilter.getTraceValidOutputPort(), this.traceThroughputFilter.getInputPort());
		connectIntraThreads(this.traceThroughputFilter.getOutputPort(), this.traceCounter.getInputPort());
		// connectIntraThreads(traceReconstructionFilter.getTraceValidOutputPort(), this.traceCounter.getInputPort());
		connectIntraThreads(this.traceCounter.getOutputPort(), endStage.getInputPort());

		connectBoundedInterThreads(clockStage.getNewOutputPort(), this.recordThroughputFilter.getTriggerInputPort(), 10);
		connectBoundedInterThreads(clock2Stage.getNewOutputPort(), this.traceThroughputFilter.getTriggerInputPort(), 10);

		return tcpReader;
	}

	public int getNumRecords() {
		return this.recordCounter.getNumElementsPassed();
	}

	public int getNumTraces() {
		return this.traceCounter.getNumElementsPassed();
	}

	public List<Long> getRecordThroughputs() {
		return this.recordThroughputFilter.getThroughputs();
	}

	public List<Long> getTraceThroughputs() {
		return this.traceThroughputFilter.getThroughputs();
	}

}
