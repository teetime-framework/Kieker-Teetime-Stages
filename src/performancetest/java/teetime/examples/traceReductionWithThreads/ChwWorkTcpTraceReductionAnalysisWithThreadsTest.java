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
package teetime.examples.traceReductionWithThreads;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import teetime.framework.Execution;
import teetime.util.ListUtil;
import teetime.util.StatisticsUtil;
import teetime.util.StopWatch;
import util.test.MooBenchStarter;

/**
 * @author Christian Wulf
 *
 * @since 1.10
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ChwWorkTcpTraceReductionAnalysisWithThreadsTest {

	private static final int EXPECTED_NUM_TRACES = 1000000;
	private static final int EXPECTED_NUM_SAME_TRACES = 1;

	private static MooBenchStarter mooBenchStarter;

	private StopWatch stopWatch;

	@BeforeClass
	public static void beforeClass() {
		mooBenchStarter = new MooBenchStarter();
	}

	@Before
	public void before() {
		this.stopWatch = new StopWatch();
	}

	@After
	public void after() {
		long overallDurationInNs = this.stopWatch.getDurationInNs();
		System.out.println("Duration: " + TimeUnit.NANOSECONDS.toMillis(overallDurationInNs) + " ms");
	}

	@Test
	public void performAnalysisWith1Thread() throws InterruptedException, IOException {
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				ChwWorkTcpTraceReductionAnalysisWithThreadsTest.this.performAnalysis(1);
			}
		};
		this.startTest(runnable);
	}

	@Test
	public void performAnalysisWith2Threads() throws InterruptedException, IOException {
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				ChwWorkTcpTraceReductionAnalysisWithThreadsTest.this.performAnalysis(2);
			}
		};
		this.startTest(runnable);
	}

	@Test
	public void performAnalysisWith4Threads() throws InterruptedException, IOException {
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				ChwWorkTcpTraceReductionAnalysisWithThreadsTest.this.performAnalysis(4);
			}
		};
		this.startTest(runnable);
	}

	void performAnalysis(final int numWorkerThreads) {
		final TcpTraceReductionAnalysisWithThreadsConfiguration configuration = new TcpTraceReductionAnalysisWithThreadsConfiguration(numWorkerThreads);

		Execution<TcpTraceReductionAnalysisWithThreadsConfiguration> analysis;
		analysis = new Execution<TcpTraceReductionAnalysisWithThreadsConfiguration>(configuration);

		this.stopWatch.start();
		try {
			analysis.executeBlocking();
		} finally {
			this.stopWatch.end();
		}

		System.out.println("#waits of tcp-relay pipe: " + configuration.getMaxNumWaits());
		// System.out.println("#traceMetadata read: " + analysis.getNumTraceMetadatas());
		// System.out.println("Max #trace created: " + analysis.getMaxElementsCreated());
		System.out.println("TraceThroughputs: " + configuration.getTraceThroughputs());

		// Map<Double, Long> recordQuintiles = StatisticsUtil.calculateQuintiles(analysis.getRecordDelays());
		// System.out.println("Median record delay: " + recordQuintiles.get(0.5) + " time units/record");

		// Map<Double, Long> traceQuintiles = StatisticsUtil.calculateQuintiles(analysis.getTraceDelays());
		// System.out.println("Median trace delay: " + traceQuintiles.get(0.5) + " time units/trace");

		List<Long> traceThroughputs = ListUtil.removeFirstHalfElements(configuration.getTraceThroughputs());
		Map<Double, Long> traceQuintiles = StatisticsUtil.calculateQuintiles(traceThroughputs);
		System.out.println("Median trace throughput: " + traceQuintiles.get(0.5) + " traces/time unit");

		assertEquals("#records", 21000001, configuration.getNumRecords());

		for (Integer count : configuration.getNumTraceMetadatas()) {
			assertEquals("#traceMetadata per worker thread", EXPECTED_NUM_TRACES / numWorkerThreads, count.intValue()); // even distribution
		}

		assertEquals("#traces", EXPECTED_NUM_SAME_TRACES, configuration.getNumTraces());
	}

	private void startTest(final Runnable runnable) throws InterruptedException, IOException {
		Thread thread = new Thread(runnable);
		thread.start();

		Thread.sleep(1000);

		mooBenchStarter.start(1, EXPECTED_NUM_TRACES);

		thread.join();
	}

	public static void main(final String[] args) {
		ChwWorkTcpTraceReductionAnalysisWithThreadsTest analysisTest = new ChwWorkTcpTraceReductionAnalysisWithThreadsTest();
		analysisTest.before();
		try {
			analysisTest.performAnalysisWith1Thread();
		} catch (Exception e) {
			System.err.println(e);
		}
		analysisTest.after();

		analysisTest.before();
		try {
			analysisTest.performAnalysisWith2Threads();
		} catch (Exception e) {
			System.err.println(e);
		}
		analysisTest.after();

		analysisTest.before();
		try {
			analysisTest.performAnalysisWith4Threads();
		} catch (Exception e) {
			System.err.println(e);
		}
		analysisTest.after();
	}

}
