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
package teetime.examples.traceReconstructionWithThreads;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import teetime.framework.AbstractStage;
import teetime.framework.Configuration;
import teetime.framework.Pipeline;
import teetime.framework.Stage;
import teetime.framework.pipe.IMonitorablePipe;
import teetime.stage.Clock;
import teetime.stage.Counter;
import teetime.stage.ElementDelayMeasuringStage;
import teetime.stage.ElementThroughputMeasuringStage;
import teetime.stage.InstanceCounter;
import teetime.stage.InstanceOfFilter;
import teetime.stage.Relay;
import teetime.stage.basic.Sink;
import teetime.stage.basic.distributor.Distributor;
import teetime.stage.io.network.TcpReaderStage;
import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.stage.trace.traceReconstruction.EventBasedTraceFactory;
import teetime.stage.trace.traceReconstruction.TraceReconstructionFilter;
import teetime.util.ConcurrentHashMapWithDefault;

import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;
import kieker.common.record.flow.trace.TraceMetadata;

public class TcpTraceReconstructionAnalysisWithThreadsConfiguration extends Configuration {

	private static final int NUM_VIRTUAL_CORES = Runtime.getRuntime().availableProcessors();
	private static final int MIO = 1000000;
	private static final int TCP_RELAY_MAX_SIZE = 2 * MIO;

	private final List<EventBasedTrace> elementCollection = new LinkedList<EventBasedTrace>();

	private final int numWorkerThreads;

	private final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace = new ConcurrentHashMapWithDefault<Long, EventBasedTrace>(
			EventBasedTraceFactory.INSTANCE);

	private final StageFactory<Counter<IMonitoringRecord>> recordCounterFactory;
	private final StageFactory<ElementDelayMeasuringStage<IMonitoringRecord>> recordDelayFilterFactory;
	private final StageFactory<ElementThroughputMeasuringStage<IMonitoringRecord>> recordThroughputFilterFactory;
	private final StageFactory<InstanceCounter<IMonitoringRecord, TraceMetadata>> traceMetadataCounterFactory;
	private final StageFactory<TraceReconstructionFilter> traceReconstructionFilterFactory;
	private final StageFactory<Counter<EventBasedTrace>> traceCounterFactory;
	private final StageFactory<ElementThroughputMeasuringStage<EventBasedTrace>> traceThroughputFilterFactory;

	private final List<IMonitorablePipe> tcpRelayPipes = new LinkedList<IMonitorablePipe>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public TcpTraceReconstructionAnalysisWithThreadsConfiguration(final int numWorkerThreads) {
		super();
		this.numWorkerThreads = Math.min(NUM_VIRTUAL_CORES, numWorkerThreads);

		try {
			this.recordCounterFactory = new StageFactory(Counter.class.getConstructor());
			this.recordDelayFilterFactory = new StageFactory(ElementDelayMeasuringStage.class.getConstructor());
			this.recordThroughputFilterFactory = new StageFactory(ElementThroughputMeasuringStage.class.getConstructor());
			this.traceMetadataCounterFactory = new StageFactory(InstanceCounter.class.getConstructor(Class.class));
			this.traceReconstructionFilterFactory = new StageFactory(TraceReconstructionFilter.class.getConstructor(ConcurrentHashMapWithDefault.class));
			this.traceCounterFactory = new StageFactory(Counter.class.getConstructor());
			this.traceThroughputFilterFactory = new StageFactory(ElementThroughputMeasuringStage.class.getConstructor());
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e);
		}

		init();
	}

	private void init() {
		Pipeline<Distributor<IMonitoringRecord>> tcpPipeline = this.buildTcpPipeline();
		declareActive(tcpPipeline.getFirstStage());

		Pipeline<Distributor<Long>> clockStage = this.buildClockPipeline(1000);
		declareActive(clockStage.getFirstStage());

		Pipeline<Distributor<Long>> clock2Stage = this.buildClockPipeline(2000);
		declareActive(clock2Stage.getFirstStage());

		for (int i = 0; i < this.numWorkerThreads; i++) {
			Stage pipeline = this.buildPipeline(tcpPipeline.getLastStage(), clockStage.getLastStage(), clock2Stage.getLastStage());
			declareActive(pipeline);
		}
	}

	private Pipeline<Distributor<IMonitoringRecord>> buildTcpPipeline() {
		TcpReaderStage tcpReader = new TcpReaderStage();
		Distributor<IMonitoringRecord> distributor = new Distributor<IMonitoringRecord>();

		connectPorts(tcpReader.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<IMonitoringRecord>>(tcpReader, distributor);
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		Clock clock = new Clock();
		clock.setInitialDelayInMs(intervalDelayInMs);
		clock.setIntervalDelayInMs(intervalDelayInMs);
		Distributor<Long> distributor = new Distributor<Long>();

		connectPorts(clock.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<Long>>(clock, distributor);
	}

	private static class StageFactory<T extends AbstractStage> {

		private final Constructor<T> constructor;
		private final List<T> stages = new ArrayList<T>();

		public StageFactory(final Constructor<T> constructor) {
			this.constructor = constructor;
		}

		public T create(final Object... initargs) {
			try {
				T stage = this.constructor.newInstance(initargs);
				this.stages.add(stage);
				return stage;
			} catch (InstantiationException e) {
				throw new IllegalStateException(e);
			} catch (IllegalAccessException e) {
				throw new IllegalStateException(e);
			} catch (IllegalArgumentException e) {
				throw new IllegalStateException(e);
			} catch (InvocationTargetException e) {
				throw new IllegalStateException(e);
			}
		}

		public List<T> getStages() {
			return this.stages;
		}
	}

	private Stage buildPipeline(final Distributor<IMonitoringRecord> tcpReaderPipeline, final Distributor<Long> clockStage,
			final Distributor<Long> clock2Stage) {
		// create stages
		Relay<IMonitoringRecord> relay = new Relay<IMonitoringRecord>();
		Counter<IMonitoringRecord> recordCounter = this.recordCounterFactory.create();
		ElementThroughputMeasuringStage<IMonitoringRecord> recordThroughputFilter = this.recordThroughputFilterFactory.create();
		// ElementDelayMeasuringStage<IMonitoringRecord> recordThroughputFilter = this.recordDelayFilterFactory.create();
		InstanceCounter<IMonitoringRecord, TraceMetadata> traceMetadataCounter = this.traceMetadataCounterFactory.create(TraceMetadata.class);
		new InstanceCounter<IMonitoringRecord, TraceMetadata>(TraceMetadata.class);
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		final TraceReconstructionFilter traceReconstructionFilter = this.traceReconstructionFilterFactory.create(this.traceId2trace);
		Counter<EventBasedTrace> traceCounter = this.traceCounterFactory.create();
		ElementThroughputMeasuringStage<EventBasedTrace> traceThroughputFilter = this.traceThroughputFilterFactory.create();
		Sink<EventBasedTrace> endStage = new Sink<EventBasedTrace>();
		// EndStage<IMonitoringRecord> endStage = new EndStage<IMonitoringRecord>();

		// connect stages
		/* TODO: IPipe tcpRelayPipe = */connectPorts(tcpReaderPipeline.getNewOutputPort(), relay.getInputPort(), TCP_RELAY_MAX_SIZE);
		// this.tcpRelayPipes.add((IMonitorablePipe) tcpRelayPipe);
		// SysOutFilter<EventBasedTrace> sysout = new SysOutFilter<EventBasedTrace>(tcpRelayPipe);

		connectPorts(clockStage.getNewOutputPort(), recordThroughputFilter.getTriggerInputPort(), 10);
		connectPorts(clock2Stage.getNewOutputPort(), traceThroughputFilter.getTriggerInputPort(), 10);

		connectPorts(relay.getOutputPort(), recordCounter.getInputPort());
		connectPorts(recordCounter.getOutputPort(), recordThroughputFilter.getInputPort());
		connectPorts(recordThroughputFilter.getOutputPort(), traceMetadataCounter.getInputPort());
		connectPorts(traceMetadataCounter.getOutputPort(), instanceOfFilter.getInputPort());
		connectPorts(instanceOfFilter.getMatchedOutputPort(), traceReconstructionFilter.getInputPort());
		connectPorts(traceReconstructionFilter.getTraceValidOutputPort(), traceCounter.getInputPort());
		// connectPorts(traceReconstructionFilter.getOutputPort(), traceThroughputFilter.getInputPort());
		// connectPorts(traceThroughputFilter.getOutputPort(), traceCounter.getInputPort());
		connectPorts(traceCounter.getOutputPort(), endStage.getInputPort());

		return relay;
	}

	public List<EventBasedTrace> getElementCollection() {
		return this.elementCollection;
	}

	public int getNumRecords() {
		int sum = 0;
		for (Counter<IMonitoringRecord> stage : this.recordCounterFactory.getStages()) {
			sum += stage.getNumElementsPassed();
		}
		return sum;
	}

	public int getNumTraces() {
		int sum = 0;
		for (Counter<EventBasedTrace> stage : this.traceCounterFactory.getStages()) {
			sum += stage.getNumElementsPassed();
		}
		return sum;
	}

	public List<Long> getRecordDelays() {
		final List<Long> throughputs = new LinkedList<Long>();
		for (ElementDelayMeasuringStage<IMonitoringRecord> stage : this.recordDelayFilterFactory.getStages()) {
			throughputs.addAll(stage.getDelays());
		}
		return throughputs;
	}

	public List<Long> getRecordThroughputs() {
		List<Long> throughputs = new LinkedList<Long>();
		for (ElementThroughputMeasuringStage<IMonitoringRecord> stage : this.recordThroughputFilterFactory.getStages()) {
			throughputs.addAll(stage.getThroughputs());
		}
		return throughputs;
	}

	public List<Long> getTraceThroughputs() {
		List<Long> throughputs = new LinkedList<Long>();
		for (ElementThroughputMeasuringStage<EventBasedTrace> stage : this.traceThroughputFilterFactory.getStages()) {
			throughputs.addAll(stage.getThroughputs());
		}
		return throughputs;
	}

	public List<Integer> getNumTraceMetadatas() {
		List<Integer> numTraceMetadatas = new LinkedList<Integer>();
		for (InstanceCounter<IMonitoringRecord, TraceMetadata> stage : this.traceMetadataCounterFactory.getStages()) {
			numTraceMetadatas.add(stage.getCounter());
		}
		return numTraceMetadatas;
	}

	public int getMaxNumWaits() {
		int maxNumWaits = 0;
		for (IMonitorablePipe pipe : this.tcpRelayPipes) {
			maxNumWaits = Math.max(maxNumWaits, pipe.getNumWaits());
		}
		return maxNumWaits;
	}

	public int getNumWorkerThreads() {
		return this.numWorkerThreads;
	}

	public int getMaxElementsCreated() {
		return this.traceId2trace.getMaxElements();
	}

}
