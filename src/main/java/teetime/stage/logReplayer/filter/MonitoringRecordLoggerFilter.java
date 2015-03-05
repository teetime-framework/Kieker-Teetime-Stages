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

package teetime.stage.logReplayer.filter;

import teetime.framework.AbstractConsumerStage;
import teetime.framework.OutputPort;

import kieker.common.configuration.Configuration;
import kieker.common.record.IMonitoringRecord;
import kieker.monitoring.core.configuration.ConfigurationFactory;
import kieker.monitoring.core.controller.IMonitoringController;
import kieker.monitoring.core.controller.MonitoringController;

/**
 * Passes {@link IMonitoringRecord}s received via its input port {@link #INPUT_PORT_NAME_RECORD} to its own {@link IMonitoringController} instance,
 * which is created based on the {@link Configuration} file passed via the filter's property {@link #CONFIG_PROPERTY_NAME_MONITORING_PROPS_FN}.
 * Additionally, incoming records are relayed via the output port {@link #OUTPUT_PORT_NAME_RELAYED_EVENTS}.
 *
 * @author Andre van Hoorn, Arne Jan Salveter
 *
 * @since 1.6
 */
public class MonitoringRecordLoggerFilter extends AbstractConsumerStage<IMonitoringRecord> {

	/** Output port that delivering the MonitoringRecordLoggerFilters. */
	private final OutputPort<IMonitoringRecord> outputPort = this.createOutputPort();

	/** The {@link IMonitoringController} the records received via {@link #inputIMonitoringRecord(IMonitoringRecord)} are passed to. */
	private final IMonitoringController monitoringController;

	/** Used to cache the configuration. */
	private final Configuration configuration;

	public OutputPort<IMonitoringRecord> getOutputPort() {
		return outputPort;
	}

	public IMonitoringController getMonitoringController() {
		return monitoringController;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public MonitoringRecordLoggerFilter(final Configuration configuration, final String monitoringPropertiesFilename) {
		final Configuration controllerConfiguration;
		final String monitoringPropertiesFn = configuration.getPathProperty(monitoringPropertiesFilename);

		if (monitoringPropertiesFn.length() > 0) {

			controllerConfiguration = ConfigurationFactory.createConfigurationFromFile(monitoringPropertiesFn);

		} else {

			this.logger.info("No path to a 'monitoring.properties' file passed; using default configuration");
			controllerConfiguration = ConfigurationFactory.createDefaultConfiguration();

		}

		// just remember this configuration without the added MonitoringController configuration
		this.configuration = (Configuration) configuration.flatten().clone();
		configuration.flatten().setDefaultConfiguration(controllerConfiguration);
		this.monitoringController = MonitoringController.createInstance(configuration.flatten());
	}

	@Override
	public void onTerminating() throws Exception {
		this.monitoringController.terminateMonitoring();
		super.onTerminating();
	}

	/**
	 * The new records are send to the monitoring controller before they are delivered via the output port.
	 *
	 * @param record
	 *            The next record.
	 */
	@Override
	protected void execute(final IMonitoringRecord element) {
		this.monitoringController.newMonitoringRecord(element);
		this.outputPort.send(element);
	}
}
