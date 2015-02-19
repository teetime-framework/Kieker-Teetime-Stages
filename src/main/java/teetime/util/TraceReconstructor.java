package teetime.util;

import teetime.stage.trace.traceReconstruction.EventBasedTrace;
import teetime.util.concurrent.hashmap.ConcurrentHashMapWithDefault;

import kieker.common.record.flow.IFlowRecord;
import kieker.common.record.flow.trace.AbstractTraceEvent;
import kieker.common.record.flow.trace.TraceMetadata;

/**
 * Utility class with methods containing the logic for trace reconstruction
 *
 * @author Nelson Tavares de Sousa
 *
 */
public class TraceReconstructor {

	private TraceReconstructor() {
		// Utility class
	}

	private static Long reconstructTrace(final IFlowRecord record, final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace) {
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

	private static void put(final Long traceId, final boolean onlyIfFinished, final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace,
			final ISendTraceBuffer sender) {
		final EventBasedTrace traceBuffer = traceId2trace.get(traceId);
		if (traceBuffer != null) { // null-check to check whether the trace has already been sent and removed
			boolean shouldSend;
			if (onlyIfFinished) {
				shouldSend = traceBuffer.isFinished();
			} else {
				shouldSend = true;
			}

			if (shouldSend) {
				boolean removed = (null != traceId2trace.remove(traceId));
				if (removed) {
					sender.sendTraceBuffer(traceBuffer);
				}
			}
		}
	}

	public static void terminate(final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace,
			final ISendTraceBuffer sender) {
		for (Long traceId : traceId2trace.keySet()) {
			put(traceId, false, traceId2trace, sender);
		}
	}

	public static void execute(final IFlowRecord record, final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace, final ISendTraceBuffer sender) {
		final Long traceId = TraceReconstructor.reconstructTrace(record, traceId2trace);
		if (traceId != null) {
			put(traceId, true, traceId2trace, sender);
		}
	}
}
