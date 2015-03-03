/**
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
package teetime.stage.opad.filter;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static teetime.framework.test.StageTester.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import kieker.tools.opad.record.StorableDetectionResult;

/**
 * @author Arne Jan Salveter
 */
public class AnomalyDetectionFilterTest {

	private AnomalyDetectionFilter adf;
	private StorableDetectionResult input1;
	private StorableDetectionResult input2;
	private StorableDetectionResult input3;
	private StorableDetectionResult input4;
	private StorableDetectionResult input5;

	List<StorableDetectionResult> resultsNormalPort;
	List<StorableDetectionResult> resultsAnnormalPort;

	@Before
	public void initializeAnomalyDetectionFilterAndInputs() {
		adf = new AnomalyDetectionFilter(6);
		input1 = new StorableDetectionResult("Test1", 1, 1, 1, 1);
		input2 = new StorableDetectionResult("Test2", 2, 1, 1, 1);
		input3 = new StorableDetectionResult("Test3", 6, 1, 1, 1);
		input4 = new StorableDetectionResult("Test4", 7, 1, 1, 1);
		input5 = new StorableDetectionResult("Test4", 10, 1, 1, 1);

		resultsNormalPort = new ArrayList<StorableDetectionResult>();
		resultsAnnormalPort = new ArrayList<StorableDetectionResult>();
	}

	@Test
	public void OutputPortNormalShouldForwardElements() {
		test(adf).and().send(input1, input2).to(adf.getInputPort()).and().receive(resultsNormalPort).from(adf.getOutputPortNormal()).start();
		assertThat(resultsNormalPort, contains(input1, input2));

		test(adf).and().send(input1, input2).to(adf.getInputPort()).and().receive(resultsAnnormalPort).from(adf.getOutputPortAnnormal()).start();
		assertThat(resultsAnnormalPort, is(empty()));
	}

	@Test
	@Ignore("maybe a problem in; org.hamcrest.collection.IsIterableContainingInOrder.contains")
	public void OutputPortAnnormalShouldForwardElements() {
		test(adf).and().send(input3, input4).to(adf.getInputPort()).and().receive(resultsNormalPort).from(adf.getOutputPortNormal()).start();
		assertEquals(0, resultsAnnormalPort.size());

		test(adf).and().send(input3, input4).to(adf.getInputPort()).and().receive(resultsAnnormalPort).from(adf.getOutputPortAnnormal()).start();
		assertThat(resultsAnnormalPort, contains(input3, input4));
	}

	@Test
	public void bothOutputPortsShouldForwardElements() {
		test(adf).and().send(input1, input2, input3, input4).to(adf.getInputPort()).and().receive(resultsNormalPort).from(adf.getOutputPortNormal()).start();
		assertThat(resultsNormalPort, is(not(empty())));
		assertThat(resultsNormalPort, contains(input1, input2));

		test(adf).and().send(input1, input2, input3, input4, input5).to(adf.getInputPort()).and().receive(resultsAnnormalPort).from(adf.getOutputPortAnnormal())
				.start();
		assertThat(resultsAnnormalPort, is(not(empty())));
		assertThat(resultsAnnormalPort, contains(input3, input4, input5));
	}
}
