/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.protocol.impl.record.value.timer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.zeebe.msgpack.property.IntegerProperty;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import io.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.zeebe.protocol.record.value.TimerRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceRelated;
import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;

public class TimerRecord extends UnifiedRecordValue
    implements WorkflowInstanceRelated, TimerRecordValue {

  private final LongProperty elementInstanceKeyProp = new LongProperty("elementInstanceKey");
  private final LongProperty workflowInstanceKeyProp = new LongProperty("workflowInstanceKey");
  private final LongProperty dueDateProp = new LongProperty("dueDate");
  private final StringProperty targetElementId = new StringProperty("targetElementId");
  private final IntegerProperty repetitionsProp = new IntegerProperty("repetitions");
  private final LongProperty workflowKeyProp = new LongProperty("workflowKey");

  public TimerRecord() {
    this.declareProperty(elementInstanceKeyProp)
        .declareProperty(workflowInstanceKeyProp)
        .declareProperty(dueDateProp)
        .declareProperty(targetElementId)
        .declareProperty(repetitionsProp)
        .declareProperty(workflowKeyProp);
  }

  @JsonIgnore
  public DirectBuffer getTargetElementIdBuffer() {
    return targetElementId.getValue();
  }

  public long getWorkflowInstanceKey() {
    return workflowInstanceKeyProp.getValue();
  }

  public TimerRecord setWorkflowInstanceKey(long workflowInstanceKey) {
    this.workflowInstanceKeyProp.setValue(workflowInstanceKey);
    return this;
  }

  @Override
  public long getWorkflowKey() {
    return workflowKeyProp.getValue();
  }

  @Override
  public long getElementInstanceKey() {
    return elementInstanceKeyProp.getValue();
  }

  @Override
  public long getDueDate() {
    return dueDateProp.getValue();
  }

  @Override
  public String getTargetElementId() {
    return BufferUtil.bufferAsString(targetElementId.getValue());
  }

  @Override
  public int getRepetitions() {
    return repetitionsProp.getValue();
  }

  public TimerRecord setRepetitions(int repetitions) {
    this.repetitionsProp.setValue(repetitions);
    return this;
  }

  public TimerRecord setTargetElementId(DirectBuffer targetElementId) {
    this.targetElementId.setValue(targetElementId);
    return this;
  }

  public TimerRecord setDueDate(long dueDate) {
    this.dueDateProp.setValue(dueDate);
    return this;
  }

  public TimerRecord setElementInstanceKey(long key) {
    elementInstanceKeyProp.setValue(key);
    return this;
  }

  public TimerRecord setWorkflowKey(long workflowKey) {
    this.workflowKeyProp.setValue(workflowKey);
    return this;
  }
}
