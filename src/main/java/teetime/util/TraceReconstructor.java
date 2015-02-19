package teetime.util;

import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;

import kieker.common.record.flow.IFlowRecord;
import kieker.common.record.flow.trace.AbstractTraceEvent;
import kieker.common.record.flow.trace.TraceMetadata;

public class TraceReconstructor {

	private TraceReconstructor() {
		// Utility class
	}

	public static Long reconstructTrace(final IFlowRecord record, final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace) {
		Long traceId = null;
		if (record instanceof TraceMetadata) {
			traceId = ((TraceMetadata) record).getTraceId();
			EventBasedTrace traceBuffer = traceId2trace.getOrCreate(traceId);

			traceBuffer.setTrace((TraceMetadata) record);
		} else if (record instanceof AbstractTraceEvent) {
			traceId = ((AbstractTraceEvent) record).getTraceId();
			EventBasedTrace traceBuffer = traceId2trace.getOrCreate(traceId);

			traceBuffer.insertEvent((AbstractTraceEvent) record);
		}

		return traceId;
	}
}
