package teetime.framework;

import java.util.List;

import teetime.framework.signal.ISignal;
import teetime.framework.validation.InvalidPortConnection;

/**
 *
 * @author Christian Wulf
 *
 * @param <L>
 *            the type of the last stage in this pipeline
 */
// TODO Consider to move it in the framework
public final class Pipeline<L extends Stage> extends Stage {

	private final Stage firstStage;
	private final L lastStage;

	public Pipeline(final Stage firstStage, final L lastStage) {
		super();
		this.firstStage = firstStage;
		this.lastStage = lastStage;
	}

	@Override
	public TerminationStrategy getTerminationStrategy() {
		return firstStage.getTerminationStrategy();
	}

	@Override
	public void terminate() {
		firstStage.terminate();
	}

	@Override
	public boolean shouldBeTerminated() {
		return firstStage.shouldBeTerminated();
	}

	@Override
	public void executeWithPorts() {
		firstStage.executeWithPorts();
	}

	@Override
	public void onSignal(final ISignal signal, final InputPort<?> inputPort) {
		firstStage.onSignal(signal, inputPort);
	}

	@Override
	public void validateOutputPorts(final List<InvalidPortConnection> invalidPortConnections) {
		lastStage.validateOutputPorts(invalidPortConnections);
	}

	public L getLastStage() {
		return lastStage;
	}

	@Override
	protected InputPort<?>[] getInputPorts() {
		return firstStage.getInputPorts();
	}

	@Override
	protected boolean isStarted() {
		return firstStage.isStarted();
	}

}
