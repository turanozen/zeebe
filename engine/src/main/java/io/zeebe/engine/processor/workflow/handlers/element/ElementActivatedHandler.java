/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.handlers.element;

import io.zeebe.engine.processor.workflow.BpmnStepContext;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableActivity;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowNode;
import io.zeebe.engine.processor.workflow.handlers.AbstractHandler;
import io.zeebe.engine.state.instance.VariablesState;
import io.zeebe.msgpack.jsonpath.JsonPathQuery;
import io.zeebe.msgpack.query.MsgPackQueryProcessor;
import io.zeebe.msgpack.spec.MsgPackToken;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import java.util.Collections;
import org.agrona.DirectBuffer;

/**
 * Represents the "business logic" phase the element, so the base handler does nothing.
 *
 * @param <T>
 */
public class ElementActivatedHandler<T extends ExecutableFlowNode> extends AbstractHandler<T> {
  public ElementActivatedHandler() {
    this(WorkflowInstanceIntent.ELEMENT_COMPLETING);
  }

  public ElementActivatedHandler(WorkflowInstanceIntent nextState) {
    super(nextState);
  }

  @Override
  protected boolean handleState(BpmnStepContext<T> context) {
    if (context.getElement() instanceof ExecutableActivity
        && ((ExecutableActivity) context.getElement()).hasLoopCharacteristics()) {

      if (context
          .getFlowScopeInstance()
          .getValue()
          .getElementIdBuffer()
          .equals(context.getElement().getId())) {
        // an inner instance
        return true;

      } else {
        // the multi instance body

        final VariablesState variablesState = context.getElementInstanceState().getVariablesState();
        final JsonPathQuery inputCollection =
            ((ExecutableActivity) context.getElement())
                .getLoopCharacteristics()
                .getInputCollection();
        final DirectBuffer variableName = inputCollection.getVariableName();
        final DirectBuffer variablesAsDocument =
            variablesState.getVariablesAsDocument(
                context.getKey(), Collections.singleton(variableName));

        final MsgPackQueryProcessor queryProcessor = new MsgPackQueryProcessor();
        final MsgPackQueryProcessor.QueryResults results =
            queryProcessor.process(inputCollection, variablesAsDocument);

        final MsgPackToken token = results.getSingleResult().getToken();
        final int size = token.getSize();

        // TODO (saig0): handle empty input collection

        // spawn instances
        final WorkflowInstanceRecord value = context.getValue();

        for (int i = 0; i < size; i++) {
          value.setFlowScopeKey(context.getKey());
          final long elementInstanceKey =
              context.getOutput().appendNewEvent(WorkflowInstanceIntent.ELEMENT_ACTIVATING, value);

          // TODO (saig0): set input element variable
        }

        return true;
      }

    } else {
      // normal activity
      return true;
    }
  }

  @Override
  protected boolean shouldHandleState(BpmnStepContext<T> context) {
    return super.shouldHandleState(context)
        && isStateSameAsElementState(context)
        && (isRootScope(context) || isElementActive(context.getFlowScopeInstance()));
  }
}
