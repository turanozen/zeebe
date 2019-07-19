/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.job;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.value.JobBatchRecordValue;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.test.util.Strings;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class JobUpdateRetriesTest {
  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final int NEW_RETRIES = 20;
  private static final String PROCESS_ID = "process";
  private static String jobType;

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Before
  public void setup() {
    jobType = Strings.newRandomValidBpmnId();
  }

  @Test
  public void shouldUpdateRetries() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    // when
    final Record<JobRecordValue> updatedRecord =
        ENGINE.job().withKey(jobKey).withRetries(NEW_RETRIES).updateRetries();

    // then
    Assertions.assertThat(updatedRecord)
        .hasRecordType(RecordType.EVENT)
        .hasIntent(JobIntent.RETRIES_UPDATED);
    assertThat(updatedRecord.getKey()).isEqualTo(jobKey);

    Assertions.assertThat(updatedRecord.getValue())
        .hasWorker(job.getWorker())
        .hasType(job.getType())
        .hasRetries(NEW_RETRIES)
        .hasDeadline(job.getDeadline());
  }

  @Test
  public void shouldRejectUpdateRetriesIfJobNotFound() {
    // when
    final Record<JobRecordValue> jobRecord =
        ENGINE.job().withKey(123).withRetries(NEW_RETRIES).expectRejection().updateRetries();

    // then
    Assertions.assertThat(jobRecord).hasRejectionType(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectUpdateRetriesIfJobCompleted() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();

    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    ENGINE.job().withKey(jobKey).withVariables("{}").complete();

    // when
    final Record<JobRecordValue> jobRecord =
        ENGINE.job().withKey(jobKey).withRetries(NEW_RETRIES).expectRejection().updateRetries();

    // then
    Assertions.assertThat(jobRecord).hasRejectionType(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldUpdateRetriesIfJobActivated() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    // when
    final Record<JobRecordValue> response =
        ENGINE.job().withKey(jobKey).withRetries(NEW_RETRIES).updateRetries();

    // then
    assertThat(response.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.getIntent()).isEqualTo(JobIntent.RETRIES_UPDATED);
    assertThat(response.getKey()).isEqualTo(jobKey);
    assertThat(response.getValue().getRetries()).isEqualTo(NEW_RETRIES);
  }

  @Test
  public void shouldUpdateRetriesIfJobCreated() {
    // given
    final long jobKey = ENGINE.createJob(jobType, PROCESS_ID).getKey();

    // when
    final Record<JobRecordValue> response =
        ENGINE.job().withKey(jobKey).withRetries(NEW_RETRIES).updateRetries();

    // then
    assertThat(response.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.getIntent()).isEqualTo(JobIntent.RETRIES_UPDATED);
    assertThat(response.getKey()).isEqualTo(jobKey);
    assertThat(response.getValue().getRetries()).isEqualTo(NEW_RETRIES);
  }

  @Test
  public void shouldRejectUpdateRetriesIfRetriesZero() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    ENGINE.job().withKey(jobKey).withRetries(0).fail();

    // when
    final Record<JobRecordValue> jobRecord =
        ENGINE.job().withKey(jobKey).withRetries(0).expectRejection().updateRetries();

    // then
    Assertions.assertThat(jobRecord).hasRejectionType(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldRejectUpdateRetriesIfRetriesLessThanZero() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    ENGINE.job().withKey(jobKey).withRetries(0).fail();

    // when
    final Record<JobRecordValue> jobRecord =
        ENGINE.job().withKey(jobKey).withRetries(-1).expectRejection().updateRetries();

    // then
    Assertions.assertThat(jobRecord).hasRejectionType(RejectionType.INVALID_ARGUMENT);
  }
}
