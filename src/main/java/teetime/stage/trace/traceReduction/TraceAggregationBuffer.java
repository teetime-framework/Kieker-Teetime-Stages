package teetime.stage.trace.traceReduction;

import teetime.stage.trace.traceReconstruction.EventBasedTrace;

/**
 * Buffer for similar traces that are to be aggregated into a single trace.
 *
 * @author Jan Waller, Florian Biss
 */
public final class TraceAggregationBuffer {

	private final long bufferCreatedTimestampInNs;
	private final EventBasedTrace representativeTrace;

	private int countOfAggregatedTraces;

	public TraceAggregationBuffer(final EventBasedTrace trace) {
		this.bufferCreatedTimestampInNs = System.nanoTime();
		this.representativeTrace = trace;
	}

	public void count() {
		this.countOfAggregatedTraces++;
	}

	public long getBufferCreatedTimestampInNs() {
		return this.bufferCreatedTimestampInNs;
	}

	public EventBasedTrace getRepresentativeTrace() {
		return this.representativeTrace;
	}

	public int getCount() {
		return this.countOfAggregatedTraces;
	}
}
