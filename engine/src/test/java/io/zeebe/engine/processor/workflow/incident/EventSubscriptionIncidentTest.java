/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.incident;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceSubscriptionIntent;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.protocol.record.value.IncidentRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceSubscriptionRecordValue;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class EventSubscriptionIncidentTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final String MESSAGE_NAME_1 = "msg-1";
  private static final String MESSAGE_NAME_2 = "msg-2";
  private static final String CORRELATION_VARIABLE_1 = "key1";
  private static final String CORRELATION_VARIABLE_2 = "key2";
  private static final String WF_RECEIVE_TASK_ID = "wf-receive-task";
  private static final BpmnModelInstance WF_RECEIVE_TASK =
      Bpmn.createExecutableProcess(WF_RECEIVE_TASK_ID)
          .startEvent()
          .receiveTask("task")
          .message(m -> m.name(MESSAGE_NAME_1).zeebeCorrelationKey(CORRELATION_VARIABLE_1))
          .boundaryEvent(
              MESSAGE_NAME_2,
              c ->
                  c.message(
                      m -> m.name(MESSAGE_NAME_2).zeebeCorrelationKey(CORRELATION_VARIABLE_2)))
          .endEvent()
          .done();
  private static final String WF_RECEIVE_TASK_2_ID = "wf-receive-task-2";
  private static final BpmnModelInstance WF_RECEIVE_TASK_2 =
      Bpmn.createExecutableProcess(WF_RECEIVE_TASK_2_ID)
          .startEvent()
          .receiveTask("task")
          .message(m -> m.name(MESSAGE_NAME_2).zeebeCorrelationKey(CORRELATION_VARIABLE_2))
          .boundaryEvent(
              MESSAGE_NAME_1,
              c ->
                  c.message(
                      m -> m.name(MESSAGE_NAME_1).zeebeCorrelationKey(CORRELATION_VARIABLE_1)))
          .endEvent()
          .done();
  private static final String WF_EVENT_BASED_GATEWAY_ID = "wf-event-based-gateway";
  private static final BpmnModelInstance WF_EVENT_BASED_GATEWAY =
      Bpmn.createExecutableProcess(WF_EVENT_BASED_GATEWAY_ID)
          .startEvent()
          .eventBasedGateway("gateway")
          .intermediateCatchEvent(
              MESSAGE_NAME_1,
              i ->
                  i.message(
                      m -> m.name(MESSAGE_NAME_1).zeebeCorrelationKey(CORRELATION_VARIABLE_1)))
          .endEvent()
          .moveToLastGateway()
          .intermediateCatchEvent(
              MESSAGE_NAME_2,
              i ->
                  i.message(
                      m -> m.name(MESSAGE_NAME_2).zeebeCorrelationKey(CORRELATION_VARIABLE_2)))
          .endEvent()
          .done();
  private static final String WF_EVENT_BASED_GATEWAY_2_ID = "wf-event-based-gateway-2";
  private static final BpmnModelInstance WF_EVENT_BASED_GATEWAY_2 =
      Bpmn.createExecutableProcess(WF_EVENT_BASED_GATEWAY_2_ID)
          .startEvent()
          .eventBasedGateway("gateway")
          .intermediateCatchEvent(
              MESSAGE_NAME_2,
              i ->
                  i.message(
                      m -> m.name(MESSAGE_NAME_2).zeebeCorrelationKey(CORRELATION_VARIABLE_2)))
          .endEvent()
          .moveToLastGateway()
          .intermediateCatchEvent(
              MESSAGE_NAME_1,
              i ->
                  i.message(
                      m -> m.name(MESSAGE_NAME_1).zeebeCorrelationKey(CORRELATION_VARIABLE_1)))
          .endEvent()
          .done();
  private static final String WF_BOUNDARY_EVENT_ID = "wf-boundary-event";
  private static final BpmnModelInstance WF_BOUNDARY_EVENT =
      Bpmn.createExecutableProcess(WF_BOUNDARY_EVENT_ID)
          .startEvent()
          .serviceTask("task", t -> t.zeebeTaskType("test"))
          .boundaryEvent(
              MESSAGE_NAME_1,
              c ->
                  c.message(
                      m -> m.name(MESSAGE_NAME_1).zeebeCorrelationKey(CORRELATION_VARIABLE_1)))
          .endEvent()
          .moveToActivity("task")
          .boundaryEvent(
              MESSAGE_NAME_2,
              c ->
                  c.message(
                      m -> m.name(MESSAGE_NAME_2).zeebeCorrelationKey(CORRELATION_VARIABLE_2)))
          .endEvent()
          .done();
  private static final String WF_BOUNDARY_EVENT_2_ID = "wf-boundary-event-2";
  private static final BpmnModelInstance WF_BOUNDARY_EVENT_2 =
      Bpmn.createExecutableProcess(WF_BOUNDARY_EVENT_2_ID)
          .startEvent()
          .serviceTask("task", t -> t.zeebeTaskType("test"))
          .boundaryEvent(
              MESSAGE_NAME_2,
              c ->
                  c.message(
                      m -> m.name(MESSAGE_NAME_2).zeebeCorrelationKey(CORRELATION_VARIABLE_2)))
          .endEvent()
          .moveToActivity("task")
          .boundaryEvent(
              MESSAGE_NAME_1,
              c ->
                  c.message(
                      m -> m.name(MESSAGE_NAME_1).zeebeCorrelationKey(CORRELATION_VARIABLE_1)))
          .endEvent()
          .done();

  @Rule
  public RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Parameter(0)
  public String elementType;

  @Parameter(1)
  public String processId;

  @Parameter(2)
  public String elementId;

  @Parameter(3)
  public WorkflowInstanceIntent failureEventIntent;

  @Parameter(4)
  public WorkflowInstanceIntent resolvedEventIntent;

  private String correlationKey1;
  private String correlationKey2;

  @Parameters(name = "{0}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "boundary catch event (first event)",
        WF_BOUNDARY_EVENT_ID,
        "task",
        WorkflowInstanceIntent.ELEMENT_ACTIVATING,
        WorkflowInstanceIntent.ELEMENT_ACTIVATED
      },
      {
        "boundary catch event (second event)",
        WF_BOUNDARY_EVENT_2_ID,
        "task",
        WorkflowInstanceIntent.ELEMENT_ACTIVATING,
        WorkflowInstanceIntent.ELEMENT_ACTIVATED
      },
      {
        "receive task (boundary event)",
        WF_RECEIVE_TASK_ID,
        "task",
        WorkflowInstanceIntent.ELEMENT_ACTIVATING,
        WorkflowInstanceIntent.ELEMENT_ACTIVATED
      },
      {
        "receive task (task)",
        WF_RECEIVE_TASK_2_ID,
        "task",
        WorkflowInstanceIntent.ELEMENT_ACTIVATING,
        WorkflowInstanceIntent.ELEMENT_ACTIVATED
      },
      {
        "event-based gateway (first event)",
        WF_EVENT_BASED_GATEWAY_ID,
        "gateway",
        WorkflowInstanceIntent.ELEMENT_ACTIVATING,
        null
      },
      {
        "event-based gateway (second event)",
        WF_EVENT_BASED_GATEWAY_2_ID,
        "gateway",
        WorkflowInstanceIntent.ELEMENT_ACTIVATING,
        null
      }
    };
  }

  @BeforeClass
  public static void deployWorkflows() {
    for (BpmnModelInstance modelInstance :
        Arrays.asList(
            WF_RECEIVE_TASK,
            WF_RECEIVE_TASK_2,
            WF_BOUNDARY_EVENT,
            WF_BOUNDARY_EVENT_2,
            WF_EVENT_BASED_GATEWAY,
            WF_EVENT_BASED_GATEWAY_2)) {
      ENGINE.deployment().withXmlResource(modelInstance).deploy();
    }
  }

  @Before
  public void init() {
    correlationKey1 = UUID.randomUUID().toString();
    correlationKey2 = UUID.randomUUID().toString();
  }

  @Test
  public void shouldCreateIncidentIfMessageCorrelationKeyNotFound() {
    // when
    final long workflowInstanceKey =
        ENGINE
            .workflowInstance()
            .ofBpmnProcessId(processId)
            .withVariable(CORRELATION_VARIABLE_1, correlationKey1)
            .create();

    final Record<WorkflowInstanceRecordValue> failureEvent =
        RecordingExporter.workflowInstanceRecords(failureEventIntent)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .withElementId(elementId)
            .getFirst();

    // then
    final Record<IncidentRecordValue> incidentRecord =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();

    Assertions.assertThat(incidentRecord.getValue())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage(
            "Failed to extract the correlation-key by '"
                + CORRELATION_VARIABLE_2
                + "': no value found")
        .hasBpmnProcessId(processId)
        .hasWorkflowInstanceKey(workflowInstanceKey)
        .hasElementId(failureEvent.getValue().getElementId())
        .hasElementInstanceKey(failureEvent.getKey())
        .hasJobKey(-1L);
  }

  @Test
  public void shouldCreateIncidentIfMessageCorrelationKeyHasInvalidType() {
    // when
    final Map<String, Object> variables = new HashMap<>();
    variables.put(CORRELATION_VARIABLE_1, correlationKey1);
    variables.put(CORRELATION_VARIABLE_2, Arrays.asList(1, 2, 3));

    final long workflowInstanceKey =
        ENGINE.workflowInstance().ofBpmnProcessId(processId).withVariables(variables).create();

    final Record<WorkflowInstanceRecordValue> failureEvent =
        RecordingExporter.workflowInstanceRecords(failureEventIntent)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .withElementId(elementId)
            .getFirst();

    // then
    final Record<IncidentRecordValue> incidentRecord =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();

    Assertions.assertThat(incidentRecord.getValue())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage(
            "Failed to extract the correlation-key by '"
                + CORRELATION_VARIABLE_2
                + "': the value must be either a string or a number")
        .hasBpmnProcessId(processId)
        .hasWorkflowInstanceKey(workflowInstanceKey)
        .hasElementId(failureEvent.getValue().getElementId())
        .hasElementInstanceKey(failureEvent.getKey())
        .hasJobKey(-1L);
  }

  @Test
  public void shouldOpenSubscriptionsWhenIncidentIsResolved() {
    // given
    final String correlationKey1 = UUID.randomUUID().toString();
    final String correlationKey2 = UUID.randomUUID().toString();

    final long workflowInstanceKey =
        ENGINE
            .workflowInstance()
            .ofBpmnProcessId(processId)
            .withVariable(CORRELATION_VARIABLE_1, correlationKey1)
            .create();

    final Record<IncidentRecordValue> incidentCreatedRecord =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();

    // when
    final Map<String, Object> document = new HashMap<>();
    document.put(CORRELATION_VARIABLE_1, correlationKey1);
    document.put(CORRELATION_VARIABLE_2, correlationKey2);
    ENGINE
        .variables()
        .ofScope(incidentCreatedRecord.getValue().getElementInstanceKey())
        .withDocument(document)
        .update();

    ENGINE
        .incident()
        .ofInstance(workflowInstanceKey)
        .withKey(incidentCreatedRecord.getKey())
        .resolve();

    // then
    assertThat(
            RecordingExporter.workflowInstanceSubscriptionRecords(
                    WorkflowInstanceSubscriptionIntent.OPENED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .limit(2))
        .extracting(Record::getValue)
        .extracting(WorkflowInstanceSubscriptionRecordValue::getMessageName)
        .containsExactlyInAnyOrder(MESSAGE_NAME_1, MESSAGE_NAME_2);

    // and
    ENGINE.message().withName(MESSAGE_NAME_2).withCorrelationKey(correlationKey2).publish();

    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .withElementId(processId)
                .exists())
        .isTrue();
  }

  @Test
  public void shouldNotOpenSubscriptionsWhenIncidentIsCreated() {
    // given
    final long workflowInstanceKey =
        ENGINE
            .workflowInstance()
            .ofBpmnProcessId(processId)
            .withVariable(CORRELATION_VARIABLE_1, correlationKey1)
            .create();

    final Record<IncidentRecordValue> incidentCreatedRecord =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();

    // when
    final Map<String, Object> document = new HashMap<>();
    document.put(CORRELATION_VARIABLE_1, correlationKey1);
    document.put(CORRELATION_VARIABLE_2, correlationKey2);
    ENGINE
        .variables()
        .ofScope(incidentCreatedRecord.getValue().getElementInstanceKey())
        .withDocument(document)
        .update();

    ENGINE
        .incident()
        .ofInstance(workflowInstanceKey)
        .withKey(incidentCreatedRecord.getKey())
        .resolve();

    // then
    final Record<IncidentRecordValue> incidentResolvedRecord =
        RecordingExporter.incidentRecords(IncidentIntent.RESOLVED)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();

    assertThat(
            RecordingExporter.workflowInstanceSubscriptionRecords(
                    WorkflowInstanceSubscriptionIntent.OPENED)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .limit(2))
        .allMatch(r -> r.getPosition() > incidentResolvedRecord.getPosition());

    // and
    if (resolvedEventIntent != null) {
      assertThat(
              RecordingExporter.workflowInstanceRecords(resolvedEventIntent)
                  .withWorkflowInstanceKey(workflowInstanceKey)
                  .withElementId(elementId)
                  .getFirst()
                  .getPosition())
          .isGreaterThan(incidentResolvedRecord.getPosition());
    }
  }
}
