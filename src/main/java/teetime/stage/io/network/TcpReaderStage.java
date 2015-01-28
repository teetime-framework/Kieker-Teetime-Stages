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

import teetime.framework.AbstractProducerStage;
import teetime.util.network.AbstractRecordTcpReader;

import kieker.common.record.IMonitoringRecord;

/**
 * This is a reader which reads the records from a TCP port.
 *
 * @author Jan Waller, Nils Christian Ehmke, Christian Wulf
 *
 */
public class TcpReaderStage extends AbstractProducerStage<IMonitoringRecord> {

	private final AbstractRecordTcpReader recordTcpReader;

	/**
	 * Default constructor with <code>port=10133</code> and <code>bufferCapacity=65535</code>
	 */
	public TcpReaderStage() {
		this(10133, 65535);
	}

	/**
	 *
	 * @param port
	 *            accept connections on this port
	 * @param bufferCapacity
	 *            capacity of the receiving buffer
	 */
	public TcpReaderStage(final int port, final int bufferCapacity) {
		super();
		this.recordTcpReader = new AbstractRecordTcpReader(port, bufferCapacity, logger) {
			@Override
			protected void send(final IMonitoringRecord record) {
				outputPort.send(record);
			}
		};
	}

	@Override
	public void onStarting() throws Exception {
		super.onStarting();
		recordTcpReader.initialize();
	}

	@Override
	protected void execute() {
		recordTcpReader.execute();
		terminate();
	}

	@Override
	public void onTerminating() throws Exception {
		this.recordTcpReader.terminate();
		super.onTerminating();
	}

	public int getPort1() {
		return recordTcpReader.getPort();
	}

	public int getPort2() {
		return recordTcpReader.getPort2();
	}

}
