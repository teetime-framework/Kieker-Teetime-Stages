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

	public TraceReductor(final Map<EventBasedTrace, TraceAggregationBuffer> trace2buffer) {
		this.trace2buffer = trace2buffer;
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

	public void processTimeoutQueue(final long timestampInNs, final long maxCollectionDurationInNs, final ISendTraceAggregationBuffer sender) {
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

	public void terminate(final ISendTraceAggregationBuffer sender) {
		synchronized (trace2buffer) { // BETTER hide and improve synchronization in the buffer
			for (final Entry<EventBasedTrace, TraceAggregationBuffer> entry : trace2buffer.entrySet()) {
				final TraceAggregationBuffer aggregatedTrace = entry.getValue();
				sender.send(aggregatedTrace);
			}
			trace2buffer.clear();
		}
	}

}
