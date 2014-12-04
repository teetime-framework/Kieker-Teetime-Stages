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
	public void executeWithPorts() {
		classNameRegistryCreationFilter.executeWithPorts();
	}
}
