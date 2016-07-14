package teetime.stage.io.network.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;

import kieker.common.exception.RecordInstantiationException;
import kieker.common.record.AbstractMonitoringRecord;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.factory.CachedRecordFactoryCatalog;
import kieker.common.record.factory.IRecordFactory;
import kieker.common.util.registry.IRegistry;

import teetime.util.io.network.AbstractTcpReader;

public class SingleSocketRecordReader extends AbstractTcpReader {

	private static final int INT_BYTES = AbstractMonitoringRecord.TYPE_SIZE_INT;
	private static final int LONG_BYTES = AbstractMonitoringRecord.TYPE_SIZE_LONG;
	private static final Charset ENCODING = StandardCharsets.UTF_8;

	private final ReaderRegistry<String> readerRegistry = new ReaderRegistry<String>();
	private final IRegistry<String> stringRegistryWrapper;
	private final IRecordReceivedListener listener;
	private final CachedRecordFactoryCatalog recordFactories = CachedRecordFactoryCatalog.getInstance();

	public SingleSocketRecordReader(final int port, final int bufferCapacity, final Logger logger, final IRecordReceivedListener listener) {
		super(port, bufferCapacity, logger);
		this.listener = listener;
		this.stringRegistryWrapper = new StringRegistryWrapper<String>(readerRegistry);
	}

	@Override
	protected boolean onBufferReceived(final ByteBuffer buffer) {
		// identify record class
		if (buffer.remaining() < INT_BYTES) {
			return false;
		}
		final int clazzId = buffer.getInt();

		if (clazzId == -1) {
			return registerRegistryEntry(clazzId, buffer);
		} else {
			return deserializeRecord(clazzId, buffer);
		}
	}

	private boolean registerRegistryEntry(final int clazzId, final ByteBuffer buffer) {
		// identify string identifier and string length
		if (buffer.remaining() < INT_BYTES + INT_BYTES) {
			return false;
		}

		final int id = buffer.getInt();
		final int stringLength = buffer.getInt();

		if (buffer.remaining() < stringLength) {
			return false;
		}

		// old code
		// final byte[] strBytes = new byte[stringLength];
		// buffer.get(strBytes);
		// String string = new String(strBytes, ENCODING);

		// new code by chw (since 14.07.2016)
		String string = new String(buffer.array(), buffer.position(), stringLength, ENCODING);

		readerRegistry.register(id, string);
		return true;
	}

	private boolean deserializeRecord(final int clazzId, final ByteBuffer buffer) {
		final String recordClassName = this.readerRegistry.get(clazzId);

		// identify logging timestamp
		if (buffer.remaining() < LONG_BYTES) {
			return false;
		}
		final long loggingTimestamp = buffer.getLong();

		// identify record data
		final IRecordFactory<? extends IMonitoringRecord> recordFactory = this.recordFactories.get(recordClassName);
		if (buffer.remaining() < recordFactory.getRecordSizeInBytes()) {
			return false;
		}

		try {
			final IMonitoringRecord record = recordFactory.create(buffer, this.stringRegistryWrapper);
			record.setLoggingTimestamp(loggingTimestamp);

			listener.onRecordReceived(record);
		} catch (final RecordInstantiationException ex) {
			super.logger.error("Failed to create: " + recordClassName, ex);
		}

		return true;
	}

}
