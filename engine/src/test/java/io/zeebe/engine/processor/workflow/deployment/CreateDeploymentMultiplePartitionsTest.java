/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.deployment;

import static io.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.value.DeploymentRecordValue;
import io.zeebe.protocol.record.value.deployment.DeployedWorkflow;
import io.zeebe.protocol.record.value.deployment.DeploymentResource;
import io.zeebe.protocol.record.value.deployment.ResourceType;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class CreateDeploymentMultiplePartitionsTest {
  public static final String PROCESS_ID = "process";
  public static final int PARTITION_ID = DEPLOYMENT_PARTITION;
  public static final int PARTITION_COUNT = 3;
  @ClassRule public static final EngineRule ENGINE = EngineRule.multiplePartition(PARTITION_COUNT);
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();
  private static final BpmnModelInstance WORKFLOW_2 =
      Bpmn.createExecutableProcess("process2").startEvent().endEvent().done();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Test
  public void shouldCreateDeploymentOnAllPartitions() {
    // when
    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess("shouldCreateDeploymentOnAllPartitions")
            .startEvent()
            .endEvent()
            .done();
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource("process.bpmn", modelInstance).deploy();

    // then
    assertThat(deployment.getKey()).isGreaterThanOrEqualTo(0L);

    assertThat(deployment.getPartitionId()).isEqualTo(PARTITION_ID);
    assertThat(deployment.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.DISTRIBUTED);

    ENGINE
        .getPartitionIds()
        .forEach(
            partitionId ->
                assertCreatedDeploymentEventResources(
                    partitionId,
                    deployment.getKey(),
                    (createdDeployment) -> {
                      final DeploymentResource resource =
                          createdDeployment.getValue().getResources().get(0);

                      Assertions.assertThat(resource)
                          .hasResource(bpmnXml(WORKFLOW))
                          .hasResourceType(ResourceType.BPMN_XML);

                      final List<DeployedWorkflow> deployedWorkflows =
                          createdDeployment.getValue().getDeployedWorkflows();

                      assertThat(deployedWorkflows).hasSize(1);
                      Assertions.assertThat(deployedWorkflows.get(0))
                          .hasBpmnProcessId("shouldCreateDeploymentOnAllPartitions")
                          .hasVersion(1)
                          .hasWorkflowKey(getDeployedWorkflow(deployment, 0).getWorkflowKey())
                          .hasResourceName("process.bpmn");
                    }));
  }

  @Test
  public void shouldOnlyDistributeFromDeploymentPartition() {
    // when
    final long deploymentKey1 = ENGINE.deployment().withXmlResource(WORKFLOW).deploy().getKey();

    // then
    final List<Record<DeploymentRecordValue>> deploymentRecords =
        RecordingExporter.deploymentRecords()
            .withRecordKey(deploymentKey1)
            .limit(r -> r.getIntent() == DeploymentIntent.DISTRIBUTED)
            .withIntent(DeploymentIntent.DISTRIBUTE)
            .asList();

    assertThat(deploymentRecords).hasSize(1);
    assertThat(deploymentRecords.get(0).getPartitionId()).isEqualTo(DEPLOYMENT_PARTITION);
  }

  @Test
  public void shouldCreateDeploymentWithYamlResourcesOnAllPartitions() throws Exception {
    // given
    final Path yamlFile =
        Paths.get(getClass().getResource("/workflows/simple-workflow.yaml").toURI());
    final byte[] yamlWorkflow = Files.readAllBytes(yamlFile);

    // when
    final Record<DeploymentRecordValue> distributedDeployment =
        ENGINE.deployment().withYamlResource("simple-workflow.yaml", yamlWorkflow).deploy();

    // then
    assertThat(distributedDeployment.getKey()).isGreaterThanOrEqualTo(0L);

    assertThat(distributedDeployment.getPartitionId()).isEqualTo(PARTITION_ID);
    assertThat(distributedDeployment.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(distributedDeployment.getIntent()).isEqualTo(DeploymentIntent.DISTRIBUTED);

    ENGINE
        .getPartitionIds()
        .forEach(
            partitionId ->
                assertCreatedDeploymentEventResources(
                    partitionId,
                    distributedDeployment.getKey(),
                    (deploymentCreatedEvent) -> {
                      final DeploymentRecordValue deployment = deploymentCreatedEvent.getValue();
                      final DeploymentResource resource = deployment.getResources().get(0);
                      Assertions.assertThat(resource).hasResourceType(ResourceType.YAML_WORKFLOW);

                      final List<DeployedWorkflow> deployedWorkflows =
                          deployment.getDeployedWorkflows();
                      assertThat(deployedWorkflows).hasSize(1);

                      Assertions.assertThat(deployedWorkflows.get(0))
                          .hasBpmnProcessId("yaml-workflow")
                          .hasVersion(1)
                          .hasWorkflowKey(
                              getDeployedWorkflow(distributedDeployment, 0).getWorkflowKey())
                          .hasResourceName("simple-workflow.yaml");
                    }));
  }

  @Test
  public void shouldCreateDeploymentResourceWithMultipleWorkflows() {
    // given

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE
            .deployment()
            .withXmlResource("process.bpmn", WORKFLOW)
            .withXmlResource("process2.bpmn", WORKFLOW_2)
            .deploy();

    // then
    assertThat(deployment.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.DISTRIBUTED);

    final List<Record<DeploymentRecordValue>> createdDeployments =
        RecordingExporter.deploymentRecords()
            .withIntent(DeploymentIntent.CREATED)
            .withRecordKey(deployment.getKey())
            .limit(PARTITION_COUNT)
            .asList();

    assertThat(createdDeployments)
        .hasSize(PARTITION_COUNT)
        .extracting(Record::getValue)
        .flatExtracting(DeploymentRecordValue::getDeployedWorkflows)
        .extracting(DeployedWorkflow::getBpmnProcessId)
        .containsOnly("process", "process2");
  }

  @Test
  public void shouldIncrementWorkflowVersions() {
    // given
    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess("shouldIncrementWorkflowVersions")
            .startEvent()
            .endEvent()
            .done();
    final Record<DeploymentRecordValue> firstDeployment =
        ENGINE.deployment().withXmlResource("process1.bpmn", modelInstance).deploy();

    // when
    final Record<DeploymentRecordValue> secondDeployment =
        ENGINE.deployment().withXmlResource("process2.bpmn", modelInstance).deploy();

    // then
    final List<Record<DeploymentRecordValue>> firstCreatedDeployments =
        RecordingExporter.deploymentRecords()
            .withIntent(DeploymentIntent.CREATED)
            .withRecordKey(firstDeployment.getKey())
            .limit(PARTITION_COUNT)
            .asList();

    assertThat(firstCreatedDeployments)
        .hasSize(PARTITION_COUNT)
        .extracting(Record::getValue)
        .flatExtracting(DeploymentRecordValue::getDeployedWorkflows)
        .extracting(DeployedWorkflow::getVersion)
        .containsOnly(1);

    final List<Record<DeploymentRecordValue>> secondCreatedDeployments =
        RecordingExporter.deploymentRecords()
            .withIntent(DeploymentIntent.CREATED)
            .withRecordKey(secondDeployment.getKey())
            .limit(PARTITION_COUNT)
            .asList();

    assertThat(secondCreatedDeployments)
        .hasSize(PARTITION_COUNT)
        .extracting(Record::getValue)
        .flatExtracting(DeploymentRecordValue::getDeployedWorkflows)
        .extracting(DeployedWorkflow::getVersion)
        .containsOnly(2);
  }

  @Test
  public void shouldFilterDuplicateWorkflow() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("process.bpmn", WORKFLOW).deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource("process.bpmn", WORKFLOW).deploy();

    // then
    assertThat(repeated.getKey()).isGreaterThan(original.getKey());

    final List<DeployedWorkflow> originalWorkflows = original.getValue().getDeployedWorkflows();
    final List<DeployedWorkflow> repeatedWorkflows = repeated.getValue().getDeployedWorkflows();
    assertThat(repeatedWorkflows.size()).isEqualTo(originalWorkflows.size()).isOne();

    assertThat(
            RecordingExporter.deploymentRecords(DeploymentIntent.CREATE)
                .withRecordKey(repeated.getKey())
                .limit(PARTITION_COUNT - 1)
                .count())
        .isEqualTo(PARTITION_COUNT - 1);

    final List<DeployedWorkflow> repeatedWfs =
        RecordingExporter.deploymentRecords(DeploymentIntent.CREATED)
            .withRecordKey(repeated.getKey())
            .limit(PARTITION_COUNT)
            .map(r -> r.getValue().getDeployedWorkflows().get(0))
            .collect(Collectors.toList());

    assertThat(repeatedWfs.size()).isEqualTo(PARTITION_COUNT);
    repeatedWfs.forEach(repeatedWf -> assertSameResource(originalWorkflows.get(0), repeatedWf));
  }

  @Test
  public void shouldNotFilterDifferentWorkflows() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("process.bpmn", WORKFLOW).deploy();

    // when
    final BpmnModelInstance sameBpmnIdModel =
        Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource("process.bpmn", sameBpmnIdModel).deploy();

    // then
    final List<DeployedWorkflow> originalWorkflows = original.getValue().getDeployedWorkflows();
    final List<DeployedWorkflow> repeatedWorkflows = repeated.getValue().getDeployedWorkflows();
    assertThat(repeatedWorkflows.size()).isEqualTo(originalWorkflows.size()).isOne();

    assertDifferentResources(originalWorkflows.get(0), repeatedWorkflows.get(0));

    assertThat(
            RecordingExporter.deploymentRecords(DeploymentIntent.CREATE)
                .withRecordKey(repeated.getKey())
                .limit(PARTITION_COUNT - 1)
                .count())
        .isEqualTo(PARTITION_COUNT - 1);

    final List<DeployedWorkflow> repeatedWfs =
        RecordingExporter.deploymentRecords(DeploymentIntent.CREATED)
            .withRecordKey(repeated.getKey())
            .limit(PARTITION_COUNT)
            .map(r -> r.getValue().getDeployedWorkflows().get(0))
            .collect(Collectors.toList());

    assertThat(repeatedWfs.size()).isEqualTo(PARTITION_COUNT);
    repeatedWfs.forEach(
        repeatedWf -> assertDifferentResources(originalWorkflows.get(0), repeatedWf));
  }

  private void assertSameResource(
      final DeployedWorkflow original, final DeployedWorkflow repeated) {
    Assertions.assertThat(repeated)
        .hasVersion(original.getVersion())
        .hasWorkflowKey(original.getWorkflowKey())
        .hasResourceName(original.getResourceName())
        .hasBpmnProcessId(original.getBpmnProcessId());
  }

  private void assertDifferentResources(
      final DeployedWorkflow original, final DeployedWorkflow repeated) {
    assertThat(original.getWorkflowKey()).isLessThan(repeated.getWorkflowKey());
    assertThat(original.getVersion()).isLessThan(repeated.getVersion());
  }

  private byte[] bpmnXml(final BpmnModelInstance definition) {
    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    Bpmn.writeModelToStream(outStream, definition);
    return outStream.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private DeployedWorkflow getDeployedWorkflow(
      final Record<DeploymentRecordValue> record, final int offset) {
    return record.getValue().getDeployedWorkflows().get(offset);
  }

  private void assertCreatedDeploymentEventResources(
      final int expectedPartition,
      final long expectedKey,
      final Consumer<Record<DeploymentRecordValue>> deploymentAssert) {
    final Record deploymentCreatedEvent =
        RecordingExporter.deploymentRecords()
            .withPartitionId(expectedPartition)
            .withIntent(DeploymentIntent.CREATED)
            .withRecordKey(expectedKey)
            .getFirst();

    assertThat(deploymentCreatedEvent.getKey()).isEqualTo(expectedKey);
    assertThat(deploymentCreatedEvent.getPartitionId()).isEqualTo(expectedPartition);

    deploymentAssert.accept(deploymentCreatedEvent);
  }
}
