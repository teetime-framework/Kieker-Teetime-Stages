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

	private final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace;
	private final ISendTraceBuffer sender;

	public TraceReconstructor(final ConcurrentHashMapWithDefault<Long, EventBasedTrace> traceId2trace, final ISendTraceBuffer sender) {
		this.traceId2trace = traceId2trace;
		this.sender = sender;
	}

	private Long reconstructTrace(final IFlowRecord record) {
		Long traceId = null;
		if (record instanceof TraceMetadata) {
			traceId = ((TraceMetadata) record).getTraceId();
			EventBasedTrace traceBuffer = this.traceId2trace.getOrCreate(traceId);

			traceBuffer.setTrace((TraceMetadata) record);
		} else if (record instanceof AbstractTraceEvent) {
			traceId = ((AbstractTraceEvent) record).getTraceId();
			EventBasedTrace traceBuffer = this.traceId2trace.getOrCreate(traceId);

			traceBuffer.insertEvent((AbstractTraceEvent) record);
		}

		return traceId;
	}

	private void put(final Long traceId, final boolean onlyIfFinished,
			final ISendTraceBuffer sender) {
		final EventBasedTrace traceBuffer = this.traceId2trace.get(traceId);
		if (traceBuffer != null) { // null-check to check whether the trace has already been sent and removed
			boolean shouldSend;
			if (onlyIfFinished) {
				shouldSend = traceBuffer.isFinished();
			} else {
				shouldSend = true;
			}

			if (shouldSend) {
				boolean removed = (null != this.traceId2trace.remove(traceId));
				if (removed) {
					sender.sendTraceBuffer(traceBuffer);
				}
			}
		}
	}

	public void terminate() {
		for (Long traceId : traceId2trace.keySet()) {
			this.put(traceId, false, sender);
		}
	}

	public void execute(final IFlowRecord record) {
		final Long traceId = this.reconstructTrace(record);
		if (traceId != null) {
			this.put(traceId, true, sender);
		}
	}

	public ConcurrentHashMapWithDefault<Long, EventBasedTrace> getTraceId2trace() {
		return traceId2trace;
	}
}
