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
package teetime.examples.traceReductionWithThreads;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import teetime.framework.AbstractStage;
import teetime.framework.AnalysisConfiguration;
import teetime.framework.Pipeline;
import teetime.framework.Stage;
import teetime.framework.pipe.IMonitorablePipe;
import teetime.framework.pipe.IPipe;
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
import teetime.stage.trace.traceReduction.EventBasedTraceComperator;
import teetime.stage.trace.traceReduction.TraceAggregationBuffer;
import teetime.stage.trace.traceReduction.TraceReductionFilter;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;

import kieker.common.record.IMonitoringRecord;
import kieker.common.record.flow.IFlowRecord;
import kieker.common.record.flow.trace.TraceMetadata;

public class TcpTraceReductionAnalysisWithThreadsConfiguration extends AnalysisConfiguration {

	private static final int NUM_VIRTUAL_CORES = Runtime.getRuntime().availableProcessors();
	private static final int MIO = 1000000;
	private static final int TCP_RELAY_MAX_SIZE = (int) (0.5 * MIO);

	private final List<EventBasedTrace> elementCollection = new LinkedList<EventBasedTrace>();

	private final List<IMonitorablePipe> tcpRelayPipes = new ArrayList<IMonitorablePipe>();
	private final int numWorkerThreads;

	private final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace;
	private final Map<EventBasedTrace, TraceAggregationBuffer> trace2buffer;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public TcpTraceReductionAnalysisWithThreadsConfiguration(final int numWorkerThreads) {
		this.numWorkerThreads = Math.min(NUM_VIRTUAL_CORES, numWorkerThreads);

		try {
			this.recordCounterFactory = new StageFactory(Counter.class.getConstructor());
			this.recordThroughputFilterFactory = new StageFactory(ElementDelayMeasuringStage.class.getConstructor());
			this.traceMetadataCounterFactory = new StageFactory(InstanceCounter.class.getConstructor(Class.class));
			this.traceCounterFactory = new StageFactory(Counter.class.getConstructor());
			this.traceThroughputFilterFactory = new StageFactory(ElementThroughputMeasuringStage.class.getConstructor());
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e);
		}

		this.traceId2trace = new ConcurrentHashMapWithDefault<Long, EventBasedTrace>(EventBasedTraceFactory.INSTANCE);
		this.trace2buffer = new TreeMap<EventBasedTrace, TraceAggregationBuffer>(new EventBasedTraceComperator());

		init();
	}

	private void init() {
		final Pipeline<Distributor<IMonitoringRecord>> tcpPipeline = this.buildTcpPipeline();
		addThreadableStage(tcpPipeline);

		final Pipeline<Distributor<Long>> clockStage = this.buildClockPipeline(1000);
		addThreadableStage(clockStage);

		final Pipeline<Distributor<Long>> clock2Stage = this.buildClockPipeline(5000);
		addThreadableStage(clock2Stage);

		for (int i = 0; i < this.numWorkerThreads; i++) {
			final Stage pipeline = this.buildPipeline(tcpPipeline, clockStage, clock2Stage);
			addThreadableStage(pipeline);
		}
	}

	private Pipeline<Distributor<IMonitoringRecord>> buildTcpPipeline() {
		final TcpReaderStage tcpReader = new TcpReaderStage();
		final Distributor<IMonitoringRecord> distributor = new Distributor<IMonitoringRecord>();

		connectIntraThreads(tcpReader.getOutputPort(), distributor.getInputPort());

		return new Pipeline<Distributor<IMonitoringRecord>>(tcpReader, distributor);
	}

	private Pipeline<Distributor<Long>> buildClockPipeline(final long intervalDelayInMs) {
		final Clock clock = new Clock();
		clock.setInitialDelayInMs(intervalDelayInMs);
		clock.setIntervalDelayInMs(intervalDelayInMs);
		final Distributor<Long> distributor = new Distributor<Long>();

		connectIntraThreads(clock.getOutputPort(), distributor.getInputPort());

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

	private final StageFactory<Counter<IMonitoringRecord>> recordCounterFactory;
	private final StageFactory<ElementDelayMeasuringStage<IMonitoringRecord>> recordThroughputFilterFactory;
	private final StageFactory<InstanceCounter<IMonitoringRecord, TraceMetadata>> traceMetadataCounterFactory;
	private final StageFactory<Counter<TraceAggregationBuffer>> traceCounterFactory;
	private final StageFactory<ElementThroughputMeasuringStage<TraceAggregationBuffer>> traceThroughputFilterFactory;

	private Stage buildPipeline(final Pipeline<Distributor<IMonitoringRecord>> tcpPipeline, final Pipeline<Distributor<Long>> clockStage,
			final Pipeline<Distributor<Long>> clock2Stage) {
		// create stages
		Relay<IMonitoringRecord> relay = new Relay<IMonitoringRecord>();
		Counter<IMonitoringRecord> recordCounter = this.recordCounterFactory.create();
		InstanceCounter<IMonitoringRecord, TraceMetadata> traceMetadataCounter = this.traceMetadataCounterFactory.create(TraceMetadata.class);
		final InstanceOfFilter<IMonitoringRecord, IFlowRecord> instanceOfFilter = new InstanceOfFilter<IMonitoringRecord, IFlowRecord>(
				IFlowRecord.class);
		// ElementDelayMeasuringStage<IMonitoringRecord> recordThroughputFilter = this.recordThroughputFilterFactory.create();
		final TraceReconstructionFilter traceReconstructionFilter = new TraceReconstructionFilter(this.traceId2trace);
		TraceReductionFilter traceReductionFilter = new TraceReductionFilter(this.trace2buffer);
		Counter<TraceAggregationBuffer> traceCounter = this.traceCounterFactory.create();
		ElementThroughputMeasuringStage<TraceAggregationBuffer> traceThroughputFilter = this.traceThroughputFilterFactory.create();
		Sink<TraceAggregationBuffer> endStage = new Sink<TraceAggregationBuffer>();

		// connect stages
		final IPipe pipe = connectBoundedInterThreads(tcpPipeline.getLastStage().getNewOutputPort(), relay.getInputPort(), TCP_RELAY_MAX_SIZE);
		this.tcpRelayPipes.add((IMonitorablePipe) pipe);

		connectIntraThreads(relay.getOutputPort(), recordCounter.getInputPort());
		connectIntraThreads(recordCounter.getOutputPort(), traceMetadataCounter.getInputPort());
		connectIntraThreads(traceMetadataCounter.getOutputPort(), instanceOfFilter.getInputPort());
		connectIntraThreads(instanceOfFilter.getMatchedOutputPort(), traceReconstructionFilter.getInputPort());
		connectIntraThreads(traceReconstructionFilter.getTraceValidOutputPort(), traceReductionFilter.getInputPort());
		connectIntraThreads(traceReductionFilter.getOutputPort(), traceCounter.getInputPort());
		connectIntraThreads(traceCounter.getOutputPort(), traceThroughputFilter.getInputPort());
		connectIntraThreads(traceThroughputFilter.getOutputPort(), endStage.getInputPort());

		// connectIntraThreads(traceReconstructionFilter.getOutputPort(), traceThroughputFilter.getInputPort());
		// connectIntraThreads(traceThroughputFilter.getOutputPort(), endStage.getInputPort());

		connectBoundedInterThreads(clock2Stage.getLastStage().getNewOutputPort(), traceReductionFilter.getTriggerInputPort(), 10);
		connectBoundedInterThreads(clockStage.getLastStage().getNewOutputPort(), traceThroughputFilter.getTriggerInputPort(), 10);

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
		for (Counter<TraceAggregationBuffer> stage : this.traceCounterFactory.getStages()) {
			sum += stage.getNumElementsPassed();
		}
		return sum;
	}

	public List<Long> getRecordDelays() {
		List<Long> throughputs = new LinkedList<Long>();
		for (ElementDelayMeasuringStage<IMonitoringRecord> stage : this.recordThroughputFilterFactory.getStages()) {
			throughputs.addAll(stage.getDelays());
		}
		return throughputs;
	}

	public List<Long> getTraceThroughputs() {
		List<Long> throughputs = new LinkedList<Long>();
		for (ElementThroughputMeasuringStage<TraceAggregationBuffer> stage : this.traceThroughputFilterFactory.getStages()) {
			throughputs.addAll(stage.getThroughputs());
		}
		return throughputs;
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

	public List<Integer> getNumTraceMetadatas() {
		List<Integer> numTraceMetadatas = new LinkedList<Integer>();
		for (InstanceCounter<IMonitoringRecord, TraceMetadata> stage : this.traceMetadataCounterFactory.getStages()) {
			numTraceMetadatas.add(stage.getCounter());
		}
		return numTraceMetadatas;
	}

}
