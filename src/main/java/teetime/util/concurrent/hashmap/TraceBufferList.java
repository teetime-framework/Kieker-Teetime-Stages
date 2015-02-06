package teetime.util.concurrent.hashmap;

import java.util.ArrayList;
import java.util.List;

import kieker.analysis.plugin.filter.flow.TraceEventRecords;
import kieker.common.logging.Log;
import kieker.common.logging.LogFactory;
import kieker.common.record.flow.trace.AbstractTraceEvent;
import kieker.common.record.flow.trace.TraceMetadata;
import kieker.common.record.flow.trace.operation.AfterOperationEvent;
import kieker.common.record.flow.trace.operation.AfterOperationFailedEvent;
import kieker.common.record.flow.trace.operation.BeforeOperationEvent;

/**
 * The TraceBuffer is synchronized to prevent problems with concurrent access.
 *
 * @author Jan Waller
 */
public final class TraceBufferList {
	private static final Log LOG = LogFactory.getLog(TraceBufferList.class);

	private TraceMetadata trace;
	private final List<AbstractTraceEvent> events = new ArrayList<AbstractTraceEvent>(1024);

	private boolean closeable;
	private boolean damaged;
	private int openEvents;
	private int maxOrderIndex = -1;

	private long traceId = -1;

	/**
	 * Creates a new instance of this class.
	 */
	public TraceBufferList() {
		// default empty constructor
	}

	public void insertEvent(final AbstractTraceEvent event) {
		final long myTraceId = event.getTraceId();
		synchronized (this) {
			if (this.traceId == -1) {
				this.traceId = myTraceId;
			} else if (this.traceId != myTraceId) {
				LOG.error("Invalid traceId! Expected: " + this.traceId + " but found: " + myTraceId + " in event " + event.toString());
				this.damaged = true;
			}

			final int orderIndex = event.getOrderIndex();
			if (orderIndex > this.maxOrderIndex) {
				this.maxOrderIndex = orderIndex;
			}
			if (event instanceof BeforeOperationEvent) {
				if (orderIndex == 0) {
					this.closeable = true;
				}
				this.openEvents++;
			} else if (event instanceof AfterOperationEvent) {
				this.openEvents--;
			} else if (event instanceof AfterOperationFailedEvent) {
				this.openEvents--;
			}

			this.events.add(event);
		}
	}

	public void setTrace(final TraceMetadata trace) {
		final long myTraceId = trace.getTraceId();
		synchronized (this) {
			if (this.traceId == -1) {
				this.traceId = myTraceId;
			} else if (this.traceId != myTraceId) {
				LOG.error("Invalid traceId! Expected: " + this.traceId + " but found: " + myTraceId + " in trace " + trace.toString());
				this.damaged = true;
			}
			if (this.trace == null) {
				this.trace = trace;
			} else {
				LOG.error("Duplicate Trace entry for traceId " + myTraceId);
				this.damaged = true;
			}
		}
	}

	public boolean isFinished() {
		synchronized (this) {
			return this.closeable && !this.isInvalid();
		}
	}

	public boolean isInvalid() {
		synchronized (this) {
			return (this.trace == null) || this.damaged || (this.openEvents != 0) || (((this.maxOrderIndex + 1) != this.events.size()) || this.events.isEmpty());
		}
	}

	public TraceEventRecords toTraceEvents() {
		synchronized (this) {
			// BETTER do not create a new array but use the events directly. Perhaps, we should refactor the events to an array.
			final AbstractTraceEvent[] traceEvents = this.events.toArray(new AbstractTraceEvent[this.events.size()]);
			return new TraceEventRecords(this.trace, traceEvents);
		}
	}

}
