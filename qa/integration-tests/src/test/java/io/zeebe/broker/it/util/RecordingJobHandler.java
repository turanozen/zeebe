/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.util;

import io.zeebe.client.api.response.ActivatedJob;
import io.zeebe.client.api.worker.JobClient;
import io.zeebe.client.api.worker.JobHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RecordingJobHandler implements JobHandler {
  protected final JobHandler[] jobHandlers;
  protected List<ActivatedJob> handledJobs = Collections.synchronizedList(new ArrayList<>());
  protected int nextJobHandler = 0;

  public RecordingJobHandler() {
    this(
        (controller, job) -> {
          // do nothing
        });
  }

  public RecordingJobHandler(JobHandler... jobHandlers) {
    this.jobHandlers = jobHandlers;
  }

  @Override
  public void handle(JobClient client, ActivatedJob job) throws Exception {
    final JobHandler handler = jobHandlers[nextJobHandler];
    nextJobHandler = Math.min(nextJobHandler + 1, jobHandlers.length - 1);

    try {
      handler.handle(client, job);
    } finally {
      handledJobs.add(job);
    }
  }

  public List<ActivatedJob> getHandledJobs() {
    return handledJobs;
  }

  public void clear() {
    handledJobs.clear();
  }
}
