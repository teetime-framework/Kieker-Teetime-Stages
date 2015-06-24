package teetime.stage.io.filesystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import teetime.framework.AbstractConsumerStage;
import teetime.stage.className.ClassNameRegistry;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.className.MappingFileParser;

public abstract class AbstractRecordFileReader extends AbstractConsumerStage<File> {

	private final ClassNameRegistryRepository classNameRegistryRepository;
	private final MappingFileParser mappingFileParser;

	public AbstractRecordFileReader(final ClassNameRegistryRepository classNameRegistryRepository) {
		this.classNameRegistryRepository = classNameRegistryRepository;
		this.mappingFileParser = new MappingFileParser(logger);
	}

	@Override
	protected void execute(final File recordFile) {
		ClassNameRegistry classNameRegistry = getClassNameRegistry(recordFile);
		reconstructRecords(classNameRegistry);
	}

	private ClassNameRegistry getClassNameRegistry(final File recordFile) {
		final File mapFile = mappingFileParser.findMappingFile(recordFile.getParentFile());

		ClassNameRegistry classNameRegistry = classNameRegistryRepository.get(mapFile);
		if (null == classNameRegistry) {
			try {
				final FileInputStream inputStream = new FileInputStream(mapFile);
				classNameRegistry = mappingFileParser.parseFromStream(inputStream);
				classNameRegistryRepository.put(mapFile, classNameRegistry);
			} catch (FileNotFoundException e) {
				logger.error("", e);
			}
		}

		return classNameRegistry;
	}

	protected abstract void reconstructRecords(ClassNameRegistry classNameRegistry);

}
