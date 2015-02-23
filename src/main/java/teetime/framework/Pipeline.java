package teetime.framework;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Christian Wulf
 *
 * @param <L>
 *            the type of the last stage in this pipeline
 */
// TODO Consider to move it in the framework
public final class Pipeline<L extends Stage> extends CompositeStage {

	private final Stage firstStage;
	private final List<L> lastStages = new LinkedList<L>();

	public Pipeline(final Stage firstStage, final L lastStage) {
		super();
		this.firstStage = firstStage;
		this.lastStages.add(lastStage);
	}

	@Override
	protected Stage getFirstStage() {
		return firstStage;
	}

	public L getLastStage() {
		return lastStages.get(0);
	}

	@Override
	protected Collection<? extends Stage> getLastStages() {
		return lastStages;
	}

	@Override
	public void onValidating(final List<InvalidPortConnection> invalidPortConnections) {
		firstStage.onValidating(invalidPortConnections);
	}

	@Override
	public void onStarting() throws Exception {
		firstStage.onStarting();
	}

	@Override
	public void onTerminating() throws Exception {
		firstStage.onTerminating();
	}

}
