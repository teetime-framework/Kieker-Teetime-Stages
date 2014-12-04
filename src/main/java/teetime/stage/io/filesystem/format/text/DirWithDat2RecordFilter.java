package teetime.stage.io.filesystem.format.text;

import java.io.File;

import teetime.framework.AbstractStage;
import teetime.framework.InputPort;
import teetime.framework.OutputPort;
import teetime.stage.className.ClassNameRegistryCreationFilter;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.io.filesystem.format.text.file.DatFile2RecordFilter;

import kieker.common.record.IMonitoringRecord;

public class DirWithDat2RecordFilter extends AbstractStage {

	private ClassNameRegistryRepository classNameRegistryRepository;
	private final ClassNameRegistryCreationFilter classNameRegistryCreationFilter;
	private final DatFile2RecordFilter datFile2RecordFilter;

	public DirWithDat2RecordFilter(final ClassNameRegistryRepository classNameRegistryRepository) {
		this.classNameRegistryRepository = classNameRegistryRepository;

		classNameRegistryCreationFilter = new ClassNameRegistryCreationFilter(classNameRegistryRepository);
		datFile2RecordFilter = new DatFile2RecordFilter(classNameRegistryRepository);
	}

	public DirWithDat2RecordFilter() {
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
		return this.datFile2RecordFilter.getOutputPort();
	}

	@Override
	public void executeWithPorts() {
		this.classNameRegistryCreationFilter.executeWithPorts();
	}

}
