package io.zeebe.engine.processor.workflow.deployment.model.element;

import io.zeebe.msgpack.jsonpath.JsonPathQuery;
import io.zeebe.msgpack.mapping.JsonPathPointer;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;

public class LoopCharacteristics {

  private final boolean isSequential;

  private final JsonPathQuery inputCollection;
  private final JsonPathPointer inputElement;

  public LoopCharacteristics(boolean isSequential, JsonPathQuery inputCollection, JsonPathPointer inputElement) {
    this.isSequential = isSequential;
    this.inputCollection = inputCollection;
    this.inputElement = inputElement;
  }

  public boolean isSequential() {
    return isSequential;
  }

  public JsonPathQuery getInputCollection() {
    return inputCollection;
  }

  public JsonPathPointer getInputElement() {
    return inputElement;
  }

  @Override
  public String toString() {
    return "LoopCharacteristics{" +
      "isSequential=" + isSequential +
      ", inputCollection=" + bufferAsString(inputCollection.getExpression()) +
      ", inputElement=" + inputElement +
      '}';
  }
}
