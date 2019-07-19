/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.handlers.activity;

import io.zeebe.engine.processor.workflow.BpmnStepContext;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableActivity;
import io.zeebe.engine.processor.workflow.handlers.CatchEventSubscriber;
import io.zeebe.engine.processor.workflow.handlers.IOMappingHelper;
import io.zeebe.engine.processor.workflow.handlers.element.ElementActivatingHandler;
import io.zeebe.engine.state.instance.VariablesState;
import io.zeebe.msgpack.jsonpath.JsonPathQuery;
import io.zeebe.msgpack.query.MsgPackQueryProcessor;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Collections;
import org.agrona.DirectBuffer;

public class ActivityElementActivatingHandler<T extends ExecutableActivity>
    extends ElementActivatingHandler<T> {
  private final CatchEventSubscriber catchEventSubscriber;

  public ActivityElementActivatingHandler(CatchEventSubscriber catchEventSubscriber) {
    super();
    this.catchEventSubscriber = catchEventSubscriber;
  }

  public ActivityElementActivatingHandler(
      WorkflowInstanceIntent nextState, CatchEventSubscriber catchEventSubscriber) {
    super(nextState);
    this.catchEventSubscriber = catchEventSubscriber;
  }

  public ActivityElementActivatingHandler(
      WorkflowInstanceIntent nextState,
      IOMappingHelper ioMappingHelper,
      CatchEventSubscriber catchEventSubscriber) {
    super(nextState, ioMappingHelper);
    this.catchEventSubscriber = catchEventSubscriber;
  }

  @Override
  protected boolean handleState(BpmnStepContext<T> context) {

    if (context.getElement().hasLoopCharacteristics()) {

      if (context
          .getFlowScopeInstance()
          .getValue()
          .getElementId()
          .equals(context.getElement().getId())) {
        // inner instance
        return super.handleState(context);

      } else {
        // the multi-instance body

        final VariablesState variablesState = context.getElementInstanceState().getVariablesState();
        final JsonPathQuery inputCollection =
            context.getElement().getLoopCharacteristics().getInputCollection();
        final DirectBuffer variableName = inputCollection.getVariableName();
        final DirectBuffer variablesAsDocument =
            variablesState.getVariablesAsDocument(
                context.getKey(), Collections.singleton(variableName));

        final MsgPackQueryProcessor queryProcessor = new MsgPackQueryProcessor();
        final MsgPackQueryProcessor.QueryResults results =
            queryProcessor.process(inputCollection, variablesAsDocument);

        if (results.size() == 0) {
          context.raiseIncident(
              ErrorType.EXTRACT_VALUE_ERROR,
              String.format(
                  "Multi-Instance variable '%s' not found.",
                  BufferUtil.bufferAsString(variableName)));
          return false;

        } else if (!results.getSingleResult().isArray()) {
          context.raiseIncident(
              ErrorType.EXTRACT_VALUE_ERROR,
              String.format(
                  "Multi-Instance variable '%s' is not an array.",
                  BufferUtil.bufferAsString(variableName)));
          return false;
        }

        return true;
      }

    } else {
      // normal activity
      return super.handleState(context) && catchEventSubscriber.subscribeToEvents(context);
    }
  }
}
