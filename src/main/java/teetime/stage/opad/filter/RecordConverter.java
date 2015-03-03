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

package teetime.stage.opad.filter;

import teetime.framework.AbstractConsumerStage;
import teetime.framework.OutputPort;

import kieker.common.record.controlflow.OperationExecutionRecord;
import kieker.tools.opad.record.NamedDoubleRecord;

/**
 * Converts OperationExecutionRecords to NamedDoubleRecords.
 *
 * @author Thomas Duellmann, Arne Jan Salveter
 * @since 1.10
 *
 */
public class RecordConverter extends AbstractConsumerStage<OperationExecutionRecord> {

	/**
	 * The output port delivers the converted OperationExecutionRecord to a NamedDoubleRecord (ndr).
	 */
	private final OutputPort<NamedDoubleRecord> outputPortNdr = this.createOutputPort();

	/**
	 * @return the outputPortNormal
	 */
	public OutputPort<NamedDoubleRecord> getOutputPortNdr() {
		return outputPortNdr;
	}

	// public RecordConverter() {
	// super();
	// }

	@Override
	protected void execute(final OperationExecutionRecord oer) {

		final String applicationName = oer.getHostname(); // reicht diese Bezeichnung?
		final long timestamp = oer.getTin(); // berücksichtigt nur die Startzeit einer Methode
		final double responseTime = oer.getTout() - oer.getTin();

		final NamedDoubleRecord ndr = new NamedDoubleRecord(applicationName, timestamp, responseTime);

		getOutputPortNdr().send(ndr);

	}
}
