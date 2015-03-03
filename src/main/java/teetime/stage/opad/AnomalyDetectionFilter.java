a/**
 * Copyright (C) 2015 TeeTime (http://teetime.sourceforge.net)
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

package teetime.stage.opad;

import teetime.framework.AbstractConsumerStage;
import teetime.framework.InputPort;
import teetime.framework.OutputPort;

/**
 *
 * This filter separates input values by their reach of a certain threshold (parameter). It takes events of type {@link StorableDetectionResult} and channels them
 * into two output ports, depending on whether the threshold was reached or not. This filter has configuration properties for the (critical) threshold. Although the
 * configuration of the critical threshold is possible, the value is currently not used by the filter.
 *
 * @author original by: Tillmann Carlos Bielefeld, Thomas Duellmann, Tobias Rudolph
 * @author edit by: Arne Jan Salveter
 * @since 1.10
 *
 */
public class AnomalyDetectionFilter<T> extends AbstractConsumerStage<T> {

	private double limit = 0;
	private final OutputPort<T> outputPortNormal = this.createOutputPort();
	private final OutputPort<T> outputPortAnnomal = this.createOutputPort();
	private final OutputPort<T> outputPortAll = this.createOutputPort();

	private final InputPort<T> inputport = this.createInputPort();

	// _____________End-Attributs__________________________________________________________________________________

	// ______________End-Constructos_____________________________________________________________________

	public void setLimit(final double limit) {
		this.limit = limit;
	}

	// ______________End-Getter/Setter_____________________________________________________________________________

	@Override
	protected void execute(final T element) {

		if ((StorableDetectionResult) element.getValue() >= limit) {

			outputPortAnnomal.send(element);

		} else {

			outputPortNormal.send(element);

		}

		outputPortAll.send(element);

	}
}