package teetime.stage.opad.filter;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import kieker.common.record.controlflow.OperationExecutionRecord;
import kieker.tools.opad.record.NamedDoubleRecord;

public class RecordConverterTest {

	private RecordConverter recordConverter;
	private OperationExecutionRecord input1;
	private OperationExecutionRecord input2;
	private OperationExecutionRecord input3;
	private OperationExecutionRecord input4;
	private OperationExecutionRecord input5;

	private NamedDoubleRecord output1;
	private NamedDoubleRecord output2;
	private NamedDoubleRecord output3;
	private NamedDoubleRecord output4;
	private NamedDoubleRecord output5;

	List<OperationExecutionRecord> resultsNdrOutputPort;

	@Before
	public void initializeRecordConverterAndInputsOutputs() {

		recordConverter = new RecordConverter();

		// Was sind eoi und esi?
		input1 = new OperationExecutionRecord("Signature-1", "TestID-1", 100, 50, 100, "Host-1", 1, 1);
		input2 = new OperationExecutionRecord("Signature-2", "TestID-2", 101, 1, 20, "Host-2", 1, 1);
		input3 = new OperationExecutionRecord("Signature-3", "TestID-3", 102, 1, 2, "Host-3", 1, 1);
		input4 = new OperationExecutionRecord("Signature-4", "TestID-4", 103, 1, 1, "Host-4", 1, 1);
		input5 = new OperationExecutionRecord("Signature-5", "TestID-5", 104, 100, 1, "Host-5", 1, 1);

		// final NamedDoubleRecord ndr = new NamedDoubleRecord(applicationName, timestamp, responseTime);
		output1 = new NamedDoubleRecord("Host-1", 50, 50);
		output2 = new NamedDoubleRecord("Host-1", 1, 19);
		output3 = new NamedDoubleRecord("Host-1", 1, 1);
		output4 = new NamedDoubleRecord("Host-1", 1, 0);
		output5 = new NamedDoubleRecord("Host-1", 100, -99);

		resultsNdrOutputPort = new ArrayList<OperationExecutionRecord>();

	}

	@Test
	@Ignore("not now")
	public void OutputPortNdrShouldForwardConvertedElements() {

		// test(recordConverter).and().send(input1, input2, input3, input4,
		// input5).to(recordConverter.getInputPort()).and().receive(resultsNdrOutputPort).from(recordConverter.getOutputPortNdr()()).start();
		//
		// assertThat(resultsNdrOutputPort, contains(input1, input2, input3, input4, input5));

	}

}
