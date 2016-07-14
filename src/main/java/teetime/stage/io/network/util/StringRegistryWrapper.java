package teetime.stage.io.network.util;

import kieker.common.util.registry.IMonitoringRecordReceiver;
import kieker.common.util.registry.IRegistry;

public class StringRegistryWrapper<E> implements IRegistry<E> {

	private final ReaderRegistry<E> readerRegistry;

	public StringRegistryWrapper(final ReaderRegistry<E> readerRegistry) {
		this.readerRegistry = readerRegistry;
	}

	@Override
	public int get(final E value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public E get(final int key) {
		return readerRegistry.get(key);
	}

	@Override
	public E[] getAll() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getSize() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRecordReceiver(final IMonitoringRecordReceiver recordReceiver) {
		throw new UnsupportedOperationException();
	}

}
