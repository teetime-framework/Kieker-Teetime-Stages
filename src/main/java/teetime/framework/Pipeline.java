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
package teetime.framework;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Christian Wulf
 *
 * @param <L>
 *            the type of the last stage in this pipeline
 */
// TODO Consider to move it in the framework
public final class Pipeline<L extends AbstractStage> extends CompositeStage {

	private final AbstractStage firstStage;
	private final List<L> lastStages = new LinkedList<L>();

	public Pipeline(final AbstractStage firstStage, final L lastStage) {
		this.firstStage = firstStage;
		this.lastStages.add(lastStage);
	}

	public AbstractStage getFirstStage() {
		return firstStage;
	}

	public L getLastStage() {
		return lastStages.get(0);
	}

}
