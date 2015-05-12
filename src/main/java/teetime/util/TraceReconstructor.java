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
