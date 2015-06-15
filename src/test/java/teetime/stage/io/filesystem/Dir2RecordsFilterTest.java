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
package teetime.stage.io.filesystem;

import java.io.File;

import org.junit.Test;

import teetime.framework.AbstractCompositeStage;
import teetime.framework.Analysis;
import teetime.framework.AnalysisConfiguration;
import teetime.framework.OutputPort;
import teetime.framework.Stage;
import teetime.stage.InitialElementProducer;
import teetime.stage.className.ClassNameRegistryRepository;
import teetime.stage.io.Printer;

import kieker.common.record.IMonitoringRecord;

public class Dir2RecordsFilterTest {

	class TestConfiguration extends AnalysisConfiguration {

		public TestConfiguration() {
			final ReadingComposite reader = new ReadingComposite(new File("."));
			final Printer<IMonitoringRecord> printer = new Printer<IMonitoringRecord>();

			super.connectIntraThreads(reader.getOutputPort(), printer.getInputPort());
			super.addThreadableStage(reader);
		}
	}

	@SuppressWarnings("deprecation")
	class ReadingComposite extends AbstractCompositeStage {

		private final InitialElementProducer<File> producer;
		private final Dir2RecordsFilter reader;

		public ReadingComposite(final File importDirectory) {
			this.producer = new InitialElementProducer<File>(importDirectory);
			this.reader = new Dir2RecordsFilter(new ClassNameRegistryRepository());

			super.connectPorts(this.producer.getOutputPort(), this.reader.getInputPort());
		}

		public OutputPort<IMonitoringRecord> getOutputPort() {
			return this.reader.getOutputPort();
		}

		@Override
		protected Stage getFirstStage() {
			return this.producer;
		}

	}

	@Test
	public void shouldNotThrowAnyException() {
		new Analysis<TestConfiguration>(new TestConfiguration()).executeBlocking();
	}
}
