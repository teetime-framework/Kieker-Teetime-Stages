/**
 * Copyright (C) 2015 Christian Wulf, Nelson Tavares de Sousa (http://teetime-framework.github.io)
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
package teetime.stage.opad.filter;

import teetime.framework.AbstractConsumerStage;
import teetime.framework.OutputPort;

import kieker.tools.opad.record.StorableDetectionResult;

/**
 *
 * This filter separates input values by their reach of a certain threshold (parameter). It takes events of type {@link StorableDetectionResult} and channels them
 * into the output port, depending on whether the threshold was reached or not. This filter has configuration properties for the (critical) threshold. Although the
 * configuration of the critical threshold is possible, the value is currently not used by the filter.
 *
 * @author Tillmann Carlos Bielefeld, Thomas Duellmann, Tobias Rudolph, Arne Jan Salveter
 * @since 1.10
 *
 */
public class AnomalyDetectionFilter extends AbstractConsumerStage<StorableDetectionResult> {

	/** The output port delivering the abnormal score if it exceeds or equals the threshold. */
	private final OutputPort<StorableDetectionResult> outputPortAnomal = this.createOutputPort();

	/** The output port delivering the normal score if it exceeds or equals the threshold. */
	private final OutputPort<StorableDetectionResult> outputPortNomal = this.createOutputPort();

	private final double threshold;

	public OutputPort<StorableDetectionResult> getOutputPortAbnormal() {
		return outputPortAnomal;
	}

	public OutputPort<StorableDetectionResult> getOutputPortRegular() {
		return outputPortNomal;
	}

	public double getThreshold() {
		return threshold;
	}

	/** @param threshold */
	public AnomalyDetectionFilter(final double threshold) {
		super();
		this.threshold = threshold;
	}

	@Override
	protected void execute(final StorableDetectionResult element) {

		if (element.getValue() >= threshold) {
			this.outputPortAnomal.send(element);
		} else {
			this.outputPortNomal.send(element);

		}

	}

}
