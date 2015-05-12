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
package teetime.stage.trace.traceReduction;

import java.util.Map;

import teetime.framework.AbstractConsumerStage;
import teetime.framework.InputPort;
import teetime.framework.OutputPort;
import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.util.ISendTraceAggregationBuffer;
import teetime.util.TraceReductor;

/**
 * This filter collects incoming traces for a specified amount of time.
 * Any traces representing the same series of events will be used to calculate statistical informations like the average runtime of this kind of trace.
 * Only one specimen of these traces containing this information will be forwarded from this filter.
 *
 * Statistical outliers regarding the runtime of the trace will be treated special and therefore send out as they are and will not be mixed with others.
 *
 * @author Jan Waller, Florian Biss
 *
 * @since
 */
public class TraceReductionFilter extends AbstractConsumerStage<EventBasedTrace> implements ISendTraceAggregationBuffer {

	private final InputPort<Long> triggerInputPort = this.createInputPort();
	private final OutputPort<TraceAggregationBuffer> outputPort = this.createOutputPort();

	private final TraceReductor reductor;

	private long maxCollectionDurationInNs;

	public TraceReductionFilter(final Map<EventBasedTrace, TraceAggregationBuffer> trace2buffer) {
		this.reductor = new TraceReductor(trace2buffer, this);
	}

	@Override
	protected void execute(final EventBasedTrace eventBasedTrace) {
		Long timestampInNs = this.triggerInputPort.receive();
		if (timestampInNs != null) {
			reductor.processTimeoutQueue(timestampInNs, maxCollectionDurationInNs);
		}

		reductor.countSameTraces(eventBasedTrace);
	}

	@Override
	public void onTerminating() throws Exception {
		reductor.terminate();

		// BETTER re-use processTimeoutQueue here, e.g., as follows
		// triggerInputPort.getPipe().add(new Date().getTime());
		// executeWithPorts();

		super.onTerminating();
	}

	@Override
	public void send(final TraceAggregationBuffer traceBuffer) {
		outputPort.send(traceBuffer);
	}

	public long getMaxCollectionDuration() {
		return this.maxCollectionDurationInNs;
	}

	public void setMaxCollectionDuration(final long maxCollectionDuration) {
		this.maxCollectionDurationInNs = maxCollectionDuration;
	}

	public InputPort<Long> getTriggerInputPort() {
		return this.triggerInputPort;
	}

	public OutputPort<TraceAggregationBuffer> getOutputPort() {
		return this.outputPort;
	}
}
