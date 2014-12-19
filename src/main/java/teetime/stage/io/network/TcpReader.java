/***************************************************************************
 * Copyright 2014 Kieker Project (http://kieker-monitoring.net)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/
package teetime.stage.io.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import teetime.stage.io.AbstractTcpReader;

import com.google.common.base.Joiner;

import kieker.common.exception.RecordInstantiationException;
import kieker.common.logging.Log;
import kieker.common.logging.LogFactory;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.factory.CachedRecordFactoryCatalog;
import kieker.common.record.factory.IRecordFactory;
import kieker.common.record.misc.RegistryRecord;
import kieker.common.util.registry.ILookup;
import kieker.common.util.registry.Lookup;

/**
 * This is a reader which reads the records from a TCP port.
 *
 * @author Jan Waller, Nils Christian Ehmke, Christian Wulf
 *
 */
public class TcpReader extends AbstractTcpReader<IMonitoringRecord> {

	private final CachedRecordFactoryCatalog recordFactories = CachedRecordFactoryCatalog.getInstance();
	// BETTER use a non thread-safe implementation to increase performance. A thread-safe version is not necessary.
	private final ILookup<String> stringRegistry = new Lookup<String>();
	private int port2 = 10134;
	private TCPStringReader tcpStringReader;

	/**
	 * Default constructor with <code>port=10133</code> and <code>bufferCapacity=65535</code>
	 */
	public TcpReader() {
		this(10133, 65535);
	}

	/**
	 *
	 * @param port
	 *            accept connections on this port
	 * @param bufferCapacity
	 *            capacity of the receiving buffer
	 */
	public TcpReader(final int port, final int bufferCapacity) {
		super(port, bufferCapacity);
	}

	@Override
	public void onStarting() throws Exception {
		super.onStarting();
		this.tcpStringReader = new TCPStringReader(this.port2, this.stringRegistry);
		this.tcpStringReader.start();
	}

	@Override
	protected final void read(final ByteBuffer buffer) {
		final int clazzId = buffer.getInt();
		final long loggingTimestamp = buffer.getLong();

		final String recordClassName = this.stringRegistry.get(clazzId);
		try {
			final IRecordFactory<? extends IMonitoringRecord> recordFactory = this.recordFactories.get(recordClassName);
			IMonitoringRecord record = recordFactory.create(buffer, this.stringRegistry);
			record.setLoggingTimestamp(loggingTimestamp);

			outputPort.send(record);
		} catch (final BufferUnderflowException ex) {
			super.logger.error("Failed to create: " + recordClassName, ex);
		} catch (final RecordInstantiationException ex) {
			super.logger.error("Failed to create: " + recordClassName, ex);
		}
	}

	@Override
	public void onTerminating() throws Exception {
		this.tcpStringReader.terminate();
		super.onTerminating();
	}

	/**
	 *
	 * @author Jan Waller
	 *
	 * @since 1.8
	 */
	private static class TCPStringReader extends Thread {

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
							System.out.println("NEW: " + Joiner.on("\n").join(stringRegistry.getAll()));
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
