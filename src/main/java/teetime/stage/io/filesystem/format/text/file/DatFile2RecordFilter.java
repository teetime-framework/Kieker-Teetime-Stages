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
package teetime.stage.io.filesystem.format.text.file;

import java.io.File;

import teetime.framework.AbstractStage;
import teetime.framework.InputPort;
import teetime.framework.OutputPort;
import teetime.framework.pipe.SingleElementPipe;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.io.File2TextLinesFilter;

import kieker.common.record.IMonitoringRecord;

/**
 * @author Christian Wulf
 *
 * @since 1.10
 */
public class DatFile2RecordFilter extends AbstractStage {

	private final File2TextLinesFilter file2TextLinesFilter;
	private final TextLine2RecordFilter textLine2RecordFilter;

	public DatFile2RecordFilter(final ClassNameRegistryRepository classNameRegistryRepository) {
		file2TextLinesFilter = new File2TextLinesFilter();
		textLine2RecordFilter = new TextLine2RecordFilter(classNameRegistryRepository);

		// BETTER let the framework choose the optimal pipe implementation
		SingleElementPipe.connect(file2TextLinesFilter.getOutputPort(), textLine2RecordFilter.getInputPort());
	}

	public InputPort<File> getInputPort() {
		return this.file2TextLinesFilter.getInputPort();
	}

	public OutputPort<IMonitoringRecord> getOutputPort() {
		return this.textLine2RecordFilter.getOutputPort();
	}

	@Override
	public void executeWithPorts() {
		file2TextLinesFilter.executeWithPorts();
	}

}
