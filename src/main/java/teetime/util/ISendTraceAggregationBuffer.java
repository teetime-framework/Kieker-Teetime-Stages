package teetime.util;

import teetime.stage.trace.traceReduction.TraceAggregationBuffer;

public interface ISendTraceAggregationBuffer {

	public void send(final TraceAggregationBuffer record);

}
