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
package teetime.examples.recordReader;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import teetime.framework.AnalysisConfiguration;
import teetime.framework.Stage;
import teetime.stage.CollectorSink;
import teetime.stage.InitialElementProducer;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.io.filesystem.Dir2RecordsFilter;

import kieker.common.record.IMonitoringRecord;

/**
 * @author Christian Wulf
 *
 * @since 1.0
 */
public class RecordReaderConfiguration extends AnalysisConfiguration {

	private final List<IMonitoringRecord> elementCollection = new LinkedList<IMonitoringRecord>();

	public RecordReaderConfiguration() {
		this.buildConfiguration();
	}

	private void buildConfiguration() {
		final Stage producerPipeline = this.buildProducerPipeline();
		addThreadableStage(producerPipeline);
	}

	private Stage buildProducerPipeline() {
		ClassNameRegistryRepository classNameRegistryRepository = new ClassNameRegistryRepository();
		File logDir = new File("src/test/data/bookstore-logs");
		// create stages
		InitialElementProducer<File> initialElementProducer = new InitialElementProducer<File>(logDir);
		Dir2RecordsFilter dir2RecordsFilter = new Dir2RecordsFilter(classNameRegistryRepository);
		CollectorSink<IMonitoringRecord> collector = new CollectorSink<IMonitoringRecord>(this.elementCollection);

		// connect stages
		connectIntraThreads(initialElementProducer.getOutputPort(), dir2RecordsFilter.getInputPort());
		connectIntraThreads(dir2RecordsFilter.getOutputPort(), collector.getInputPort());

		return initialElementProducer;
	}

	public List<IMonitoringRecord> getElementCollection() {
		return this.elementCollection;
	}

}
