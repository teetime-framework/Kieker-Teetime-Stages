/**
 * Copyright (C) 2015 Christian Wulf, Nelson Tavares de Sousa (http://teetime-framework.github.io)
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
