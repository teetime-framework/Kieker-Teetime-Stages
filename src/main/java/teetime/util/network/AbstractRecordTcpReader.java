package teetime.util.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;

import kieker.common.exception.RecordInstantiationException;
import kieker.common.logging.Log;
import kieker.common.logging.LogFactory;
import kieker.common.record.AbstractMonitoringRecord;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.factory.CachedRecordFactoryCatalog;
import kieker.common.record.factory.IRecordFactory;
import kieker.common.record.misc.RegistryRecord;
import kieker.common.util.registry.ILookup;
import kieker.common.util.registry.Lookup;

public abstract class AbstractRecordTcpReader extends AbstractTcpReader {

	private static final int INT_BYTES = AbstractMonitoringRecord.TYPE_SIZE_INT;
	private static final int LONG_BYTES = AbstractMonitoringRecord.TYPE_SIZE_LONG;

	private final CachedRecordFactoryCatalog recordFactories = CachedRecordFactoryCatalog.getInstance();
	// BETTER use a non thread-safe implementation to increase performance. A thread-safe version is not necessary.
	private final ILookup<String> stringRegistry = new Lookup<String>();
	private int port2 = 10134;
	private TCPStringReader tcpStringReader;

	/**
	 * Default constructor with <code>port=10133</code> and <code>bufferCapacity=65535</code>
	 */
	public AbstractRecordTcpReader(final Logger logger) {
		this(10133, 65535, logger);
	}

	/**
	 *
	 * @param port
	 *            accept connections on this port
	 * @param bufferCapacity
	 *            capacity of the receiving buffer
	 */
	public AbstractRecordTcpReader(final int port, final int bufferCapacity, final Logger logger) {
		super(port, bufferCapacity, logger);
	}

	public void initialize() {
		this.tcpStringReader = new TCPStringReader(this.port2, this.stringRegistry);
		this.tcpStringReader.start();
	}

	@Override
	protected final boolean read(final ByteBuffer buffer) {
		// identify record class
		if (buffer.remaining() < INT_BYTES) {
			return false;
		}
		final int clazzId = buffer.getInt();
		final String recordClassName = this.stringRegistry.get(clazzId);

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
			final IMonitoringRecord record = recordFactory.create(buffer, this.stringRegistry);
			record.setLoggingTimestamp(loggingTimestamp);

			send(record);
		} catch (final RecordInstantiationException ex) {
			super.logger.error("Failed to create: " + recordClassName, ex);
		}

		return true;
	}

	protected abstract void send(IMonitoringRecord record);

	public void terminate() {
		this.tcpStringReader.terminate();
	}

	/**
	 *
	 * @author Jan Waller
	 *
	 * @since 1.8
	 */
	protected static class TCPStringReader extends Thread {

		private static final int MESSAGE_BUFFER_SIZE = 65535;

		private static final Log LOG = LogFactory.getLog(TCPStringReader.class);

		private final int port;
		private final ILookup<String> stringRegistry;
		private volatile boolean terminated = false; // NOPMD
		private volatile Thread readerThread;

		public TCPStringReader(final int port, final ILookup<String> stringRegistry) {
			this.port = port;
			this.stringRegistry = stringRegistry;
		}

		public void terminate() {
			this.terminated = true;
			this.readerThread.interrupt();
		}

		@Override
		public void run() {
			this.readerThread = Thread.currentThread();
			ServerSocketChannel serversocket = null;
			try {
				serversocket = ServerSocketChannel.open();
				serversocket.socket().bind(new InetSocketAddress(this.port));
				if (LOG.isDebugEnabled()) {
					LOG.debug("Listening on port " + this.port);
				}
				// BEGIN also loop this one?
				final SocketChannel socketChannel = serversocket.accept();
				final ByteBuffer buffer = ByteBuffer.allocateDirect(MESSAGE_BUFFER_SIZE);
				while ((socketChannel.read(buffer) != -1) && (!this.terminated)) {
					buffer.flip();
					try {
						while (buffer.hasRemaining()) {
							buffer.mark();
							RegistryRecord.registerRecordInRegistry(buffer, this.stringRegistry);
						}
						buffer.clear();
					} catch (final BufferUnderflowException ex) {
						buffer.reset();
						buffer.compact();
					}
				}
				socketChannel.close();
				// END also loop this one?
			} catch (final ClosedByInterruptException ex) {
				LOG.warn("Reader interrupted", ex);
			} catch (final IOException ex) {
				LOG.error("Error while reading", ex);
			} finally {
				if (null != serversocket) {
					try {
						serversocket.close();
					} catch (final IOException e) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Failed to close TCP connection!", e);
						}
					}
				}
			}
		}
	}

	public int getPort2() {
		return this.port2;
	}

	public void setPort2(final int port2) {
		this.port2 = port2;
	}

}
