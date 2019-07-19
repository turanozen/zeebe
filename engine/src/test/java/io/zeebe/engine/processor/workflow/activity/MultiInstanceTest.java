/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.activity;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.Arrays;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class MultiInstanceTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Test
  public void shouldDeployMultiInstanceActivity() {

    final BpmnModelInstance workflow =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "task",
                t ->
                    t.zeebeTaskType("test")
                        .multiInstance(
                            m -> m.zeebeInputCollection("items").zeebeInputElement("item")))
            .endEvent()
            .done();

    ENGINE.deployment().withXmlResource(workflow).deploy();

    final long workflowInstanceKey =
        ENGINE
            .workflowInstance()
            .ofBpmnProcessId("process")
            .withVariable("items", Arrays.asList(1, 2, 3))
            .create();

    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .withElementId("task")
                .limit(4))
        .hasSize(4);

    assertThat(
            RecordingExporter.jobRecords(JobIntent.CREATED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .limit(3))
        .hasSize(3);
  }
}
