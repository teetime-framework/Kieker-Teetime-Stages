package teetime.stage.trace.traceReduction;

import kieker.analysis.plugin.filter.flow.TraceEventRecords;

/**
 * Buffer for similar traces that are to be aggregated into a single trace.
 *
 * @author Jan Waller, Florian Biss
 */
public final class TraceAggregationBuffer {

	private final long bufferCreatedTimestampInNs;
	private final TraceEventRecords aggregatedTrace;

	private int countOfAggregatedTraces;

	public TraceAggregationBuffer(final TraceEventRecords trace) {
		this.bufferCreatedTimestampInNs = System.nanoTime();
		this.aggregatedTrace = trace;
	}

	public void count() {
		this.countOfAggregatedTraces++;
	}

	public long getBufferCreatedTimestampInNs() {
		return this.bufferCreatedTimestampInNs;
	}

	public TraceEventRecords getTraceEventRecords() {
		return this.aggregatedTrace;
	}

	public int getCount() {
		return this.countOfAggregatedTraces;
	}
}
