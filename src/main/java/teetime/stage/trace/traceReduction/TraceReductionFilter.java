/***************************************************************************
 * Copyright 2014 Kieker Project (http://kieker-monitoring.net)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

package teetime.stage.trace.traceReduction;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import teetime.framework.AbstractConsumerStage;
import teetime.framework.InputPort;
import teetime.framework.OutputPort;
import teetime.stage.trace.traceReconstruction.EventBasedTrace;

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
public class TraceReductionFilter extends AbstractConsumerStage<EventBasedTrace> {

	private final InputPort<Long> triggerInputPort = this.createInputPort();
	private final OutputPort<TraceAggregationBuffer> outputPort = this.createOutputPort();

	private final Map<EventBasedTrace, TraceAggregationBuffer> trace2buffer;

	private long maxCollectionDurationInNs;

	public TraceReductionFilter(final Map<EventBasedTrace, TraceAggregationBuffer> trace2buffer) {
		this.trace2buffer = trace2buffer;
	}

	@Override
	protected void execute(final EventBasedTrace eventBasedTrace) {
		Long timestampInNs = this.triggerInputPort.receive();
		if (timestampInNs != null) {
			this.processTimeoutQueue(timestampInNs);
		}

		this.countSameTraces(eventBasedTrace);
	}

	private void countSameTraces(final EventBasedTrace eventBasedTrace) {
		synchronized (this.trace2buffer) {
			TraceAggregationBuffer aggregatedTrace = this.trace2buffer.get(eventBasedTrace);
			if (aggregatedTrace == null) {
				aggregatedTrace = new TraceAggregationBuffer(eventBasedTrace);
				this.trace2buffer.put(eventBasedTrace, aggregatedTrace);
			}
			aggregatedTrace.count();
		}
	}

	@Override
	public void onTerminating() throws Exception {
		synchronized (this.trace2buffer) { // BETTER hide and improve synchronization in the buffer
			for (final Entry<EventBasedTrace, TraceAggregationBuffer> entry : this.trace2buffer.entrySet()) {
				final TraceAggregationBuffer traceBuffer = entry.getValue();
				send(traceBuffer);
			}
			this.trace2buffer.clear();
		}

		// BETTER re-use processTimeoutQueue here, e.g., as follows
		// triggerInputPort.getPipe().add(new Date().getTime());
		// executeWithPorts();

		super.onTerminating();
	}

	private void processTimeoutQueue(final long timestampInNs) {
		final long bufferTimeoutInNs = timestampInNs - this.maxCollectionDurationInNs;
		synchronized (this.trace2buffer) {
			for (final Iterator<Entry<EventBasedTrace, TraceAggregationBuffer>> iterator = this.trace2buffer.entrySet().iterator(); iterator.hasNext();) {
				final TraceAggregationBuffer traceBuffer = iterator.next().getValue();
				// this.logger.debug("traceBuffer.getBufferCreatedTimestamp(): " + traceBuffer.getBufferCreatedTimestamp() + " vs. " + bufferTimeoutInNs
				// + " (bufferTimeoutInNs)");
				if (traceBuffer.getBufferCreatedTimestampInNs() <= bufferTimeoutInNs) {
					send(traceBuffer);
				}
				iterator.remove();
			}
		}
	}

	private void send(final TraceAggregationBuffer traceBuffer) {
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
