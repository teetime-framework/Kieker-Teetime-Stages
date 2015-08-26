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

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import teetime.framework.Configuration;
import teetime.framework.Stage;
import teetime.stage.Cache;
import teetime.stage.Clock;
import teetime.stage.CollectorSink;
import teetime.stage.Counter;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.InitialElementProducer;
import teetime.stage.InstanceOfFilter;
import teetime.stage.basic.merger.Merger;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.io.filesystem.Dir2RecordsFilter;
import teetime.stage.string.buffer.StringBufferFilter;
import teetime.stage.string.buffer.handler.MonitoringRecordHandler;
import teetime.stage.string.buffer.handler.StringHandler;
import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.stage.trace.traceReconstruction.EventBasedTraceFactory;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.util.ConcurrentHashMapWithDefault;

import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;

public class TraceReconstructionConf extends Configuration {

	private final List<EventBasedTrace> elementCollection = new LinkedList<EventBasedTrace>();

	private final File inputDir;
	private final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace;

	private ClassNameRegistryRepository classNameRegistryRepository;
	private Counter<IMonitoringRecord> recordCounter;
	private Counter<EventBasedTrace> traceCounter;
	private ElementThroughputMeasuringStage<IFlowRecord> throughputFilter;

	public TraceReconstructionConf(final File inputDir) {
		this.inputDir = inputDir;
		this.traceId2trace = new ConcurrentHashMapWithDefault<Long, EventBasedTrace>(EventBasedTraceFactory.INSTANCE);
		init();
	}

	private void init() {
		Clock clockStage = this.buildClockPipeline();
		declareActive(clockStage);

		Stage pipeline = this.buildPipeline(clockStage);
		declareActive(pipeline);
	}

	private Clock buildClockPipeline() {
		Clock clock = new Clock();
		clock.setIntervalDelayInMs(100);

		return clock;
	}

	private Stage buildPipeline(final Clock clockStage) {
		this.classNameRegistryRepository = new ClassNameRegistryRepository();

		// create stages
		InitialElementProducer<File> initialElementProducer = new InitialElementProducer<File>(this.inputDir);
		final Dir2RecordsFilter dir2RecordsFilter = new Dir2RecordsFilter(this.classNameRegistryRepository);
		this.recordCounter = new Counter<IMonitoringRecord>();
		final Cache<IMonitoringRecord> cache = new Cache<IMonitoringRecord>();

		final StringBufferFilter<IMonitoringRecord> stringBufferFilter = new StringBufferFilter<IMonitoringRecord>();
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		this.throughputFilter = new ElementThroughputMeasuringStage<IFlowRecord>();
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		Merger<EventBasedTrace> merger = new Merger<EventBasedTrace>();
		this.traceCounter = new Counter<EventBasedTrace>();
		final CollectorSink<EventBasedTrace> collector = new CollectorSink<EventBasedTrace>(this.elementCollection);

		// configure stages
		stringBufferFilter.getDataTypeHandlers().add(new MonitoringRecordHandler());
		stringBufferFilter.getDataTypeHandlers().add(new StringHandler());

		// connect stages
		connectPorts(initialElementProducer.getOutputPort(), dir2RecordsFilter.getInputPort());
		connectPorts(dir2RecordsFilter.getOutputPort(), this.recordCounter.getInputPort());
		connectPorts(this.recordCounter.getOutputPort(), cache.getInputPort());
		connectPorts(cache.getOutputPort(), stringBufferFilter.getInputPort());
		connectPorts(stringBufferFilter.getOutputPort(), instanceOfFilter.getInputPort());
		connectPorts(instanceOfFilter.getMatchedOutputPort(), this.throughputFilter.getInputPort());
		connectPorts(this.throughputFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		// connectPorts(instanceOfFilter.getOutputPort(), traceReconstructionFilter.getInputPort());
		connectPorts(traceReconstructionFilter.getTraceValidOutputPort(), merger.getNewInputPort());
		connectPorts(traceReconstructionFilter.getTraceInvalidOutputPort(), merger.getNewInputPort());
		connectPorts(merger.getOutputPort(), this.traceCounter.getInputPort());
		connectPorts(this.traceCounter.getOutputPort(), collector.getInputPort());

		connectPorts(clockStage.getOutputPort(), this.throughputFilter.getTriggerInputPort(), 1);

		return initialElementProducer;
	}

	public List<EventBasedTrace> getElementCollection() {
		return this.elementCollection;
	}

	public int getNumRecords() {
		return this.recordCounter.getNumElementsPassed();
	}

	public int getNumTraces() {
		return this.traceCounter.getNumElementsPassed();
	}

	public List<Long> getThroughputs() {
		return this.throughputFilter.getThroughputs();
	}

	public File getInputDir() {
		return this.inputDir;
	}

}
