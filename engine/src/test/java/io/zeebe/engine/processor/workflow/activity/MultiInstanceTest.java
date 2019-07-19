package io.zeebe.engine.processor.workflow.activity;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import io.zeebe.test.util.record.WorkflowInstanceRecordStream;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiInstanceTest {

  @ClassRule
  public static final EngineRule ENGINE = EngineRule.singlePartition();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
    new RecordingExporterTestWatcher();

  @Test
  public void shouldDeployMultiInstanceActivity() {

    final BpmnModelInstance workflow = Bpmn.createExecutableProcess("process")
      .startEvent()
      .serviceTask(
        "task",
        t ->
          t.zeebeTaskType("test")
            .multiInstance(
              m -> m.zeebeInputCollection("items").zeebeInputElement("item")))
      .endEvent()
      .done();

    ENGINE
      .deployment()
      .withXmlResource(
        workflow)
      .deploy();

    final long workflowInstanceKey = ENGINE
      .workflowInstance()
      .ofBpmnProcessId("process")
      .withVariable("items", Arrays.asList(1, 2, 3))
      .create();

    final WorkflowInstanceRecordStream stream = RecordingExporter.workflowInstanceRecords().withWorkflowInstanceKey(workflowInstanceKey).withElementId("task").limitToWorkflowInstanceCompleted();

    assertThat(stream).hasSize(8);
  }
}