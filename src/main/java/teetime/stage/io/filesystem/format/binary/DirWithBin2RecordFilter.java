/**
 * Copyright (C) 2015 Christian Wulf, Nelson Tavares de Sousa (http://teetime.sourceforge.net)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package teetime.stage.io.filesystem.format.binary;

import java.io.File;

import teetime.framework.AbstractStage;
import teetime.framework.InputPort;
import teetime.framework.OutputPort;
import teetime.stage.className.ClassNameRegistryCreationFilter;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.io.filesystem.format.binary.file.BinaryFile2RecordFilter;

import kieker.common.record.IMonitoringRecord;

public class DirWithBin2RecordFilter extends AbstractStage {

	private ClassNameRegistryRepository classNameRegistryRepository;
	private final ClassNameRegistryCreationFilter classNameRegistryCreationFilter;
	private final BinaryFile2RecordFilter binaryFile2RecordFilter;

	public DirWithBin2RecordFilter(final ClassNameRegistryRepository classNameRegistryRepository) {
		this.classNameRegistryRepository = classNameRegistryRepository;

		classNameRegistryCreationFilter = new ClassNameRegistryCreationFilter(classNameRegistryRepository);
		binaryFile2RecordFilter = new BinaryFile2RecordFilter(classNameRegistryRepository);
	}

	public DirWithBin2RecordFilter() {
		this(null);
	}

	public ClassNameRegistryRepository getClassNameRegistryRepository() {
		return this.classNameRegistryRepository;
	}

	public void setClassNameRegistryRepository(final ClassNameRegistryRepository classNameRegistryRepository) {
		this.classNameRegistryRepository = classNameRegistryRepository;
	}

	public InputPort<File> getInputPort() {
		return this.classNameRegistryCreationFilter.getInputPort();
	}

	public OutputPort<IMonitoringRecord> getOutputPort() {
		return this.binaryFile2RecordFilter.getOutputPort();
	}

	@Override
	public void executeStage() {
		classNameRegistryCreationFilter.executeStage();
	}
}
