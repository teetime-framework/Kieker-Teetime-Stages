package teetime.stage.io.network.util;

import java.util.HashMap;
import java.util.Map;

// TODO move to kieker.common.registry.read
public class ReaderRegistry<E> {

	// TODO replace by a high performance map with primitive key type
	private final Map<Long, E> registryEntries = new HashMap<Long, E>();

	public E get(final long key) {
		return registryEntries.get(key);
	}

	public void register(final long key, final E value) {
		registryEntries.put(key, value);
	}
}
