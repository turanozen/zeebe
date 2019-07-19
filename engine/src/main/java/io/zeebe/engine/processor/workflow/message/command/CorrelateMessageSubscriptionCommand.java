/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.message.command;

import io.zeebe.engine.util.SbeBufferWriterReader;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class CorrelateMessageSubscriptionCommand
    extends SbeBufferWriterReader<
        CorrelateMessageSubscriptionEncoder, CorrelateMessageSubscriptionDecoder> {

  private final CorrelateMessageSubscriptionEncoder encoder =
      new CorrelateMessageSubscriptionEncoder();
  private final CorrelateMessageSubscriptionDecoder decoder =
      new CorrelateMessageSubscriptionDecoder();
  private final UnsafeBuffer messageName = new UnsafeBuffer(0, 0);
  private int subscriptionPartitionId;
  private long workflowInstanceKey;
  private long elementInstanceKey;

  @Override
  protected CorrelateMessageSubscriptionEncoder getBodyEncoder() {
    return encoder;
  }

  @Override
  protected CorrelateMessageSubscriptionDecoder getBodyDecoder() {
    return decoder;
  }

  @Override
  public void reset() {
    subscriptionPartitionId =
        CorrelateMessageSubscriptionDecoder.subscriptionPartitionIdNullValue();
    workflowInstanceKey = CorrelateMessageSubscriptionDecoder.workflowInstanceKeyNullValue();
    elementInstanceKey = CorrelateMessageSubscriptionDecoder.elementInstanceKeyNullValue();
    messageName.wrap(0, 0);
  }

  @Override
  public int getLength() {
    return super.getLength()
        + CorrelateMessageSubscriptionDecoder.messageNameHeaderLength()
        + messageName.capacity();
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    super.write(buffer, offset);

    encoder
        .subscriptionPartitionId(subscriptionPartitionId)
        .workflowInstanceKey(workflowInstanceKey)
        .elementInstanceKey(elementInstanceKey)
        .putMessageName(messageName, 0, messageName.capacity());
  }

  @Override
  public void wrap(DirectBuffer buffer, int offset, int length) {
    super.wrap(buffer, offset, length);

    subscriptionPartitionId = decoder.subscriptionPartitionId();
    workflowInstanceKey = decoder.workflowInstanceKey();
    elementInstanceKey = decoder.elementInstanceKey();

    offset = decoder.limit();

    offset += CorrelateMessageSubscriptionDecoder.messageNameHeaderLength();
    final int messageNameLength = decoder.messageNameLength();
    messageName.wrap(buffer, offset, messageNameLength);
  }

  public int getSubscriptionPartitionId() {
    return subscriptionPartitionId;
  }

  public void setSubscriptionPartitionId(int subscriptionPartitionId) {
    this.subscriptionPartitionId = subscriptionPartitionId;
  }

  public long getWorkflowInstanceKey() {
    return workflowInstanceKey;
  }

  public void setWorkflowInstanceKey(long workflowInstanceKey) {
    this.workflowInstanceKey = workflowInstanceKey;
  }

  public long getElementInstanceKey() {
    return elementInstanceKey;
  }

  public void setElementInstanceKey(long elementInstanceKey) {
    this.elementInstanceKey = elementInstanceKey;
  }

  public DirectBuffer getMessageName() {
    return messageName;
  }
}
