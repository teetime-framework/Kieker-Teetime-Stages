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
package teetime.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.stage.trace.traceReduction.TraceAggregationBuffer;

/**
 * Utility class with methods containing the logic for trace reduction
 *
 * @author Nelson Tavares de Sousa
 *
 */
public class TraceReductor {

	private final Map<EventBasedTrace, TraceAggregationBuffer> trace2buffer;
	private final ISendTraceAggregationBuffer sender;

	public TraceReductor(final Map<EventBasedTrace, TraceAggregationBuffer> trace2buffer, final ISendTraceAggregationBuffer sender) {
		this.trace2buffer = trace2buffer;
		this.sender = sender;
	}

	public void countSameTraces(final EventBasedTrace eventBasedTrace) {
		synchronized (trace2buffer) {
			TraceAggregationBuffer aggregatedTrace = trace2buffer.get(eventBasedTrace);
			if (aggregatedTrace == null) {
				aggregatedTrace = new TraceAggregationBuffer(eventBasedTrace);
				trace2buffer.put(eventBasedTrace, aggregatedTrace);
			}
			aggregatedTrace.count();
		}
	}

	public void processTimeoutQueue(final long timestampInNs, final long maxCollectionDurationInNs) {
		final long bufferTimeoutInNs = timestampInNs - maxCollectionDurationInNs;
		synchronized (trace2buffer) {
			for (final Iterator<Entry<EventBasedTrace, TraceAggregationBuffer>> iterator = trace2buffer
					.entrySet().iterator(); iterator.hasNext();) {
				final TraceAggregationBuffer traceBuffer = iterator.next().getValue();
				if (traceBuffer.getBufferCreatedTimestampInNs() <= bufferTimeoutInNs) {
					sender.send(traceBuffer);
				}
				iterator.remove();
			}
		}
	}

	public void terminate() {
		synchronized (trace2buffer) { // BETTER hide and improve synchronization in the buffer
			for (final Entry<EventBasedTrace, TraceAggregationBuffer> entry : trace2buffer.entrySet()) {
				final TraceAggregationBuffer aggregatedTrace = entry.getValue();
				sender.send(aggregatedTrace);
			}
			trace2buffer.clear();
		}
	}

}
