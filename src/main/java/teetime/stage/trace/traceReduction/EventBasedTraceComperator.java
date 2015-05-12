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
package teetime.stage.trace.traceReduction;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import teetime.stage.trace.traceReconstruction.EventBasedTrace;

import kieker.common.record.flow.trace.AbstractTraceEvent;
import kieker.common.record.flow.trace.operation.AbstractOperationEvent;
import kieker.common.record.flow.trace.operation.AfterOperationFailedEvent;

/**
 * @author Jan Waller, Florian Fittkau, Florian Biss
 */
public final class EventBasedTraceComperator implements Comparator<EventBasedTrace>, Serializable {

	private static final long serialVersionUID = 8920766818232517L;

	/**
	 * Creates a new instance of this class.
	 */
	public EventBasedTraceComperator() {
		// default empty constructor
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compare(final EventBasedTrace t1, final EventBasedTrace t2) {
		final List<AbstractTraceEvent> recordsT1 = t1.getTraceEvents();
		final List<AbstractTraceEvent> recordsT2 = t2.getTraceEvents();

		if (recordsT1.size() != recordsT2.size()) {
			return recordsT1.size() - recordsT2.size();
		}

		final int cmpHostnames = t1.getTraceMetaData().getHostname().compareTo(t2.getTraceMetaData().getHostname());
		if (cmpHostnames != 0) {
			return cmpHostnames;
		}

		for (int i = 0; i < recordsT1.size(); i++) {
			final AbstractTraceEvent recordT1 = recordsT1.get(i);
			final AbstractTraceEvent recordT2 = recordsT2.get(i);

			final int cmpClass = recordT1.getClass().getName().compareTo(recordT2.getClass().getName());
			if (cmpClass != 0) {
				return cmpClass;
			}
			if (recordT1 instanceof AbstractOperationEvent) {
				final int cmpSignature = ((AbstractOperationEvent) recordT1).getOperationSignature()
						.compareTo(((AbstractOperationEvent) recordT2).getOperationSignature());
				if (cmpSignature != 0) {
					return cmpSignature;
				}
			}
			if (recordT1 instanceof AfterOperationFailedEvent) {
				final int cmpError = ((AfterOperationFailedEvent) recordT1).getCause().compareTo(
						((AfterOperationFailedEvent) recordT2).getCause());
				if (cmpError != 0) {
					return cmpClass;
				}
			}
		}
		// All records match.
		return 0;
	}
}
