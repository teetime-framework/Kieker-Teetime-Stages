package teetime.stage.trace.traceReconstruction;

import teetime.util.concurrent.hashmap.ValueFactory;

public final class EventBasedTraceFactory implements ValueFactory<EventBasedTrace> {

	public final static EventBasedTraceFactory INSTANCE = new EventBasedTraceFactory();

	private EventBasedTraceFactory() {}

	@Override
	public EventBasedTrace create() {
		return new EventBasedTrace();
	}
}
