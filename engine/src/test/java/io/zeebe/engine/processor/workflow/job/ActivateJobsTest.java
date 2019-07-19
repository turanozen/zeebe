/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.job;

import static io.zeebe.protocol.record.Assertions.assertThat;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static io.zeebe.test.util.record.RecordingExporter.jobRecords;
import static io.zeebe.test.util.record.RecordingExporter.workflowInstanceRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.zeebe.engine.processor.workflow.message.command.VarDataEncodingEncoder;
import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.JobBatchIntent;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.JobBatchRecordValue;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.test.util.Strings;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import io.zeebe.util.sched.clock.ControlledActorClock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.internal.bytebuddy.utility.RandomString;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class ActivateJobsTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final String LONG_CUSTOM_HEADER_VALUE = RandomString.make(128);
  private static final String PROCESS_ID = "process";
  private static final Function<String, BpmnModelInstance> MODEL_SUPPLIER =
      (type) ->
          Bpmn.createExecutableProcess(PROCESS_ID)
              .startEvent("start")
              .serviceTask("task", b -> b.zeebeTaskType(type).done())
              .endEvent("end")
              .done();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  private String taskType;

  @Before
  public void setup() {
    taskType = Strings.newRandomValidBpmnId();
  }

  @Test
  public void shouldRejectInvalidAmount() {
    // when
    final Record<JobBatchRecordValue> batchRecord =
        ENGINE.jobs().withType(taskType).withMaxJobsToActivate(0).expectRejection().activate();

    // then
    Assertions.assertThat(batchRecord)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT)
        .hasRejectionReason(
            "Expected to activate job batch with max jobs to activate to be greater than zero, but it was '0'");
  }

  @Test
  public void shouldRejectInvalidTimeout() {
    // when
    final Record<JobBatchRecordValue> batchRecord =
        ENGINE
            .jobs()
            .withType(taskType)
            .withTimeout(Duration.ofSeconds(0).toMillis())
            .expectRejection()
            .activate();

    // then
    Assertions.assertThat(batchRecord)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT)
        .hasRejectionReason(
            "Expected to activate job batch with timeout to be greater than zero, but it was '0'");
  }

  @Test
  public void shouldRejectInvalidType() {
    // when
    final Record<JobBatchRecordValue> batchRecord =
        ENGINE.jobs().withType("").expectRejection().activate();

    // then
    Assertions.assertThat(batchRecord)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT)
        .hasRejectionReason(
            "Expected to activate job batch with type to be present, but it was blank");
  }

  @Test
  public void shouldRejectInvalidWorker() {
    // when
    final Record<JobBatchRecordValue> batchRecord =
        ENGINE.jobs().withType(taskType).byWorker("").expectRejection().activate();

    // then
    Assertions.assertThat(batchRecord)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT)
        .hasRejectionReason(
            "Expected to activate job batch with worker to be present, but it was blank");
  }

  @Test
  public void shouldActivateSingleJob() {
    // given
    ENGINE.deployment().withXmlResource(PROCESS_ID, MODEL_SUPPLIER.apply(taskType)).deploy();
    final long firstInstanceKey = createWorkflowInstances(3, "{'foo':'bar'}").get(0);

    final long expectedJobKey =
        jobRecords(JobIntent.CREATED)
            .withType(taskType)
            .filter(r -> r.getValue().getWorkflowInstanceKey() == firstInstanceKey)
            .getFirst()
            .getKey();

    final String worker = "myTestWorker";
    final Duration timeout = Duration.ofMinutes(12);

    // when
    final Record<JobBatchRecordValue> batchRecord =
        ENGINE
            .jobs()
            .withType(taskType)
            .byWorker(worker)
            .withTimeout(timeout.toMillis())
            .withMaxJobsToActivate(1)
            .activate();

    final List<JobRecordValue> jobs = batchRecord.getValue().getJobs();
    final List<Long> jobKeys = batchRecord.getValue().getJobKeys();

    // then
    assertThat(batchRecord.getIntent()).isEqualTo(JobBatchIntent.ACTIVATED);

    assertThat(jobKeys).hasSize(1);
    assertThat(jobs).hasSize(1);
    assertThat(jobKeys.get(0)).isEqualTo(expectedJobKey);
    assertThat(jobs.get(0)).hasRetries(3).hasWorker(worker).hasType(taskType);

    assertThat(jobs.get(0).getVariables()).containsExactly(entry("foo", "bar"));

    final Record<JobRecordValue> jobRecord =
        jobRecords(JobIntent.ACTIVATED)
            .withWorkflowInstanceKey(firstInstanceKey)
            .withType(taskType)
            .getFirst();
    assertThat(jobRecord).hasKey(expectedJobKey);
    assertThat(jobRecord.getValue()).hasRetries(3).hasWorker(worker);
  }

  @Test
  public void shouldActivateJobBatch() {
    // given
    final List<Long> expectedJobKeys = deployAndCreateJobs(taskType, 5).subList(0, 3);

    // when
    final List<Long> jobKeys = activateJobs(3);

    // then
    assertThat(jobKeys).containsExactlyInAnyOrderElementsOf(expectedJobKeys);
  }

  @Test
  public void shouldActivateJobBatches() {
    // given
    final List<Long> jobKeys = deployAndCreateJobs(taskType, 12);
    final List<Long> expectedFirstJobKeys = jobKeys.subList(0, 3);
    final List<Long> expectedSecondJobKeys = jobKeys.subList(3, 7);
    final List<Long> expectedThirdJobKeys = jobKeys.subList(7, 10);

    // when
    final List<Long> firstJobs = activateJobs(3);
    final List<Long> secondJobs = activateJobs(4);
    final List<Long> thirdJobs = activateJobs(3);

    // then
    assertThat(firstJobs).containsOnlyElementsOf(expectedFirstJobKeys);
    assertThat(secondJobs).containsOnlyElementsOf(expectedSecondJobKeys);
    assertThat(thirdJobs).containsOnlyElementsOf(expectedThirdJobKeys);
  }

  @Test
  public void shouldReturnEmptyBatchIfNoJobsAvailable() {
    // when
    final List<Long> jobEvents = activateJobs(RandomString.make(5), 3);

    // then
    assertThat(jobEvents).isEmpty();
  }

  @Test
  public void shouldCompleteActivatedJobs() {
    // given
    final int jobAmount = 5;
    final List<Long> jobKeys = deployAndCreateJobs(taskType, jobAmount);
    final List<Long> activateJobKeys = activateJobs(taskType, jobAmount);

    // when
    activateJobKeys.forEach(this::completeJob);

    // then
    final List<Record<JobRecordValue>> records =
        jobRecords(JobIntent.COMPLETED).limit(jobAmount).collect(Collectors.toList());

    assertThat(records).extracting(Record::getKey).containsOnlyElementsOf(jobKeys);
  }

  @Test
  public void shouldOnlyReturnJobsOfCorrectType() {
    // given
    final List<Long> jobKeys = deployAndCreateJobs(taskType, 3);
    deployAndCreateJobs("different" + taskType, 5);
    jobKeys.addAll(deployAndCreateJobs(taskType, 4));

    // when
    final List<Long> jobs = activateJobs(taskType, 7);

    // then
    assertThat(jobs).containsOnlyElementsOf(jobKeys);

    final List<Record<JobRecordValue>> records =
        jobRecords(JobIntent.ACTIVATED)
            .withType(taskType)
            .limit(jobKeys.size())
            .collect(Collectors.toList());

    assertThat(records).extracting(Record::getKey).containsOnlyElementsOf(jobKeys);
    assertThat(records).extracting("value.type").containsOnly(taskType);
  }

  @Test
  public void shouldActivateJobsFromWorkflow() {
    // given
    final int jobAmount = 10;
    final String jobType = taskType;
    final String jobType2 = Strings.newRandomValidBpmnId();
    final String jobType3 = Strings.newRandomValidBpmnId();

    AbstractFlowNodeBuilder<?, ?> builder =
        Bpmn.createExecutableProcess(PROCESS_ID).startEvent("start");

    for (final String type : Arrays.asList(jobType, jobType2, jobType3)) {
      builder = builder.serviceTask(type, b -> b.zeebeTaskType(type));
    }
    ENGINE.deployment().withXmlResource(PROCESS_ID, builder.done()).deploy();

    final List<Long> workflowInstanceKeys = createWorkflowInstances(jobAmount, "{}");

    // when activating and completing all jobs
    waitForJobs(jobType, jobAmount, workflowInstanceKeys);
    activateJobs(jobType, jobAmount).forEach(this::completeJob);

    waitForJobs(jobType2, jobAmount, workflowInstanceKeys);
    activateJobs(jobType2, jobAmount).forEach(this::completeJob);

    waitForJobs(jobType3, jobAmount, workflowInstanceKeys);
    activateJobs(jobType3, jobAmount).forEach(this::completeJob);

    // then all workflow instances are completed
    assertThat(
        workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
                .withBpmnProcessId(PROCESS_ID)
                .filter(r -> workflowInstanceKeys.contains(r.getKey()))
                .limit(jobAmount)
                .count()
            == workflowInstanceKeys.size());
  }

  @Test
  public void shouldActivateJobsWithLongCustomHeaders() {
    // given
    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess(PROCESS_ID)
            .startEvent()
            .serviceTask(
                "task",
                b -> b.zeebeTaskType(taskType).zeebeTaskHeader("foo", LONG_CUSTOM_HEADER_VALUE))
            .endEvent()
            .done();

    ENGINE.deployment().withXmlResource(PROCESS_ID, modelInstance).deploy();
    final long workflowInstanceKey = createWorkflowInstances(1, "{}").get(0);
    RecordingExporter.jobRecords(JobIntent.CREATED)
        .withWorkflowInstanceKey(workflowInstanceKey)
        .getFirst();

    // when
    activateJob(taskType);
    ENGINE.job().ofInstance(workflowInstanceKey).withType(taskType).complete();

    // then
    final JobRecordValue jobRecord =
        RecordingExporter.jobRecords(JobIntent.ACTIVATED)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .withType(taskType)
            .getFirst()
            .getValue();
    assertThat(jobRecord.getCustomHeaders().get("foo")).isEqualTo(LONG_CUSTOM_HEADER_VALUE);
  }

  @Test
  public void shouldFetchFullJobRecordFromWorkflow() {
    // given
    final ControlledActorClock clock = ENGINE.getClock();
    clock.pinCurrentTime();

    final String worker = "testWorker";
    final Duration timeout = Duration.ofMinutes(4);

    ENGINE.deployment().withXmlResource(PROCESS_ID, MODEL_SUPPLIER.apply(taskType)).deploy();
    createWorkflowInstances(1, "{'foo':'bar'}");
    final Record<JobRecordValue> jobRecord =
        jobRecords(JobIntent.CREATED).withType(taskType).getFirst();

    // when
    final Record<JobBatchRecordValue> jobActivatedRecord =
        ENGINE
            .jobs()
            .withType(taskType)
            .byWorker(worker)
            .withTimeout(timeout.toMillis())
            .withMaxJobsToActivate(1)
            .activate();

    final JobRecordValue jobActivated = jobActivatedRecord.getValue().getJobs().get(0);
    final Record<JobBatchRecordValue> jobActivate =
        RecordingExporter.jobBatchRecords()
            .withType(taskType)
            .withIntent(JobBatchIntent.ACTIVATE)
            .getFirst();

    // then
    Assertions.assertThat(jobActivated)
        .hasType(taskType)
        .hasWorker(worker)
        .hasRetries(3)
        .hasDeadline(jobActivate.getTimestamp() + timeout.toMillis());

    assertThat(jobActivated.getVariables()).containsExactly(entry("foo", "bar"));

    final JobRecordValue jobRecordValue = jobRecord.getValue();
    assertThat(jobActivated.getWorkflowInstanceKey())
        .isEqualTo(jobRecordValue.getWorkflowInstanceKey());
    Assertions.assertThat(jobActivated)
        .hasBpmnProcessId(jobRecordValue.getBpmnProcessId())
        .hasWorkflowDefinitionVersion(jobRecordValue.getWorkflowDefinitionVersion())
        .hasWorkflowKey(jobRecordValue.getWorkflowKey())
        .hasElementId(jobRecordValue.getElementId())
        .hasElementInstanceKey(jobRecordValue.getElementInstanceKey());

    assertThat(jobActivated.getCustomHeaders()).isEqualTo(jobRecordValue.getCustomHeaders());
  }

  @Test
  public void shouldLimitJobsInBatch() {
    // given
    final int variablesSize = VarDataEncodingEncoder.lengthMaxValue() / 3;
    final String variables = "{'key': '" + RandomString.make(variablesSize) + "'}";

    // when
    deployAndCreateJobs(taskType, 3, variables);
    final List<Long> jobKeys = activateJobs(taskType, 3);

    // then
    assertThat(jobKeys).hasSize(2);
    final List<Long> remainingJobKeys = activateJobs(1);
    assertThat(remainingJobKeys).hasSize(1);
  }

  private Record<JobRecordValue> completeJob(long jobKey) {
    return ENGINE.job().withKey(jobKey).complete();
  }

  private Long activateJob(String type) {
    return activateJobs(type, 1).get(0);
  }

  private List<Long> activateJobs(String type, int amount) {
    return ENGINE
        .jobs()
        .withType(type)
        .withMaxJobsToActivate(amount)
        .activate()
        .getValue()
        .getJobKeys();
  }

  private List<Long> activateJobs(int amount) {
    return activateJobs(taskType, amount);
  }

  private List<Long> createWorkflowInstances(int amount, String variables) {
    return IntStream.range(0, amount)
        .boxed()
        .map(
            i ->
                ENGINE
                    .workflowInstance()
                    .ofBpmnProcessId(PROCESS_ID)
                    .withVariables(variables)
                    .create())
        .collect(Collectors.toList());
  }

  private List<Long> deployAndCreateJobs(
      final String type, final int amount, final String variables) {
    ENGINE.deployment().withXmlResource(PROCESS_ID, MODEL_SUPPLIER.apply(type)).deploy();
    final List<Long> instanceKeys = createWorkflowInstances(amount, variables);

    return jobRecords(JobIntent.CREATED)
        .withType(type)
        .filter(r -> instanceKeys.contains(r.getValue().getWorkflowInstanceKey()))
        .limit(amount)
        .map(Record::getKey)
        .collect(Collectors.toList());
  }

  private List<Long> deployAndCreateJobs(String type, int amount) {
    return deployAndCreateJobs(type, amount, "{'foo':'bar'}");
  }

  private void waitForJobs(
      final String jobType, final int jobAmount, final List<Long> workflowInstanceKeys) {
    waitUntil(
        () ->
            jobRecords(JobIntent.CREATED)
                    .filter(
                        r -> workflowInstanceKeys.contains(r.getValue().getWorkflowInstanceKey()))
                    .withType(jobType)
                    .limit(jobAmount)
                    .count()
                == jobAmount);
  }
}
