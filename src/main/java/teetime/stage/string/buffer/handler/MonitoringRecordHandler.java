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
package teetime.stage.string.buffer.handler;

import kieker.common.exception.MonitoringRecordException;
import kieker.common.record.AbstractMonitoringRecord;
import kieker.common.record.IMonitoringRecord;

/**
 * @author Christian Wulf
 *
 * @since 1.0
 */
public final class MonitoringRecordHandler extends AbstractDataTypeHandler<IMonitoringRecord> {

	@Override
	public boolean canHandle(final Object object) {
		return object instanceof IMonitoringRecord;
	}

	@Override
	public IMonitoringRecord handle(final IMonitoringRecord monitoringRecord) {
		final Object[] objects = monitoringRecord.toArray();

		boolean stringBuffered = false;
		for (int i = 0; i < objects.length; i++) {
			if (objects[i] instanceof String) {
				objects[i] = this.stringRepository.get((String) objects[i]);
				stringBuffered = true;
			}
		}

		if (stringBuffered) {
			try {
				final IMonitoringRecord newRecord = AbstractMonitoringRecord.createFromArray(monitoringRecord.getClass(), objects);
				newRecord.setLoggingTimestamp(monitoringRecord.getLoggingTimestamp());
				return newRecord;
			} catch (final MonitoringRecordException ex) {
				this.logger.warn("Failed to recreate buffered monitoring record: " + monitoringRecord.toString(), ex);
			}
		}

		return monitoringRecord;
	}

}
