/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.deployment.model.element;

import org.agrona.DirectBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ExecutableActivity extends ExecutableFlowNode implements ExecutableCatchEventSupplier {
  private final List<ExecutableBoundaryEvent> boundaryEvents = new ArrayList<>();
  private final List<ExecutableCatchEvent> catchEvents = new ArrayList<>();
  private final List<DirectBuffer> interruptingIds = new ArrayList<>();

  private LoopCharacteristics loopCharacteristics;

  public ExecutableActivity(String id) {
    super(id);
  }

  public void attach(ExecutableBoundaryEvent boundaryEvent) {
    boundaryEvents.add(boundaryEvent);
    catchEvents.add(boundaryEvent);

    if (boundaryEvent.cancelActivity()) {
      interruptingIds.add(boundaryEvent.getId());
    }
  }

  public void setLoopCharacteristics(LoopCharacteristics loopCharacteristics) {
    this.loopCharacteristics = loopCharacteristics;
  }

  public LoopCharacteristics getLoopCharacteristics() {
    return loopCharacteristics;
  }

  public boolean hasLoopCharacteristics() {
    return loopCharacteristics != null;
  }

  @Override
  public List<ExecutableCatchEvent> getEvents() {
    return catchEvents;
  }

  public List<ExecutableBoundaryEvent> getBoundaryEvents() {
    return boundaryEvents;
  }

  @Override
  public Collection<DirectBuffer> getInterruptingElementIds() {
    return interruptingIds;
  }
}
