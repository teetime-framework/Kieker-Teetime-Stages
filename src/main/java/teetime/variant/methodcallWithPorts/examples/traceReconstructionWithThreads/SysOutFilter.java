package teetime.variant.methodcallWithPorts.examples.traceReconstructionWithThreads;

import teetime.variant.methodcallWithPorts.framework.core.ConsumerStage;
import teetime.variant.methodcallWithPorts.framework.core.InputPort;
import teetime.variant.methodcallWithPorts.framework.core.pipe.IPipe;

import kieker.common.record.IMonitoringRecord;

public class SysOutFilter<T> extends ConsumerStage<T, T> {

	private final InputPort<Long> triggerInputPort = new InputPort<Long>(this);

	private final IPipe<IMonitoringRecord> pipe;

	public SysOutFilter(final IPipe<IMonitoringRecord> pipe) {
		this.pipe = pipe;
	}

	@Override
	protected void execute5(final T element) {
		Long timestamp = this.triggerInputPort.receive();
		if (timestamp != null) {
			// this.logger.info("pipe.size: " + this.pipe.size());
			System.out.println("pipe.size: " + this.pipe.size());
		}
		this.send(element);
	}

	public InputPort<Long> getTriggerInputPort() {
		return this.triggerInputPort;
	}

}
