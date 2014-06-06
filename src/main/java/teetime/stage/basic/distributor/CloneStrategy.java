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
package teetime.stage.basic.distributor;

import java.util.List;

import teetime.framework.core.Context;
import teetime.framework.core.IOutputPort;

/**
 * @author Nils Christian Ehmke
 * 
 * @since 1.10
 */
public final class CloneStrategy<T> implements IDistributorStrategy<T> {

	public <S extends Distributor<T>> boolean distribute(final Context<S> context, final List<IOutputPort<S, ?>> outputPorts, final T input) {
		throw new UnsupportedOperationException();
	}

}