package teetime.stage.io.filesystem.format.text;

import java.io.File;

import teetime.framework.InputPort;
import teetime.framework.OldPipeline;
import teetime.framework.OutputPort;
import teetime.stage.className.ClassNameRegistryCreationFilter;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.io.filesystem.format.text.file.DatFile2RecordFilter;

import kieker.common.record.IMonitoringRecord;

public class DirWithDat2RecordFilter extends OldPipeline<ClassNameRegistryCreationFilter, DatFile2RecordFilter> {

	private ClassNameRegistryRepository classNameRegistryRepository;

	public DirWithDat2RecordFilter(final ClassNameRegistryRepository classNameRegistryRepository) {
		this.classNameRegistryRepository = classNameRegistryRepository;

		final ClassNameRegistryCreationFilter classNameRegistryCreationFilter = new ClassNameRegistryCreationFilter(classNameRegistryRepository);
		final DatFile2RecordFilter datFile2RecordFilter = new DatFile2RecordFilter(classNameRegistryRepository);

		this.setFirstStage(classNameRegistryCreationFilter);
		this.setLastStage(datFile2RecordFilter);
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
		return this.getFirstStage().getInputPort();
	}

	public OutputPort<IMonitoringRecord> getOutputPort() {
		return this.getLastStage().getOutputPort();
	}
}
