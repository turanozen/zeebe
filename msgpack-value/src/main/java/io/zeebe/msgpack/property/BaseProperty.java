/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.msgpack.property;

import io.zeebe.msgpack.MsgpackPropertyException;
import io.zeebe.msgpack.Recyclable;
import io.zeebe.msgpack.spec.MsgPackReader;
import io.zeebe.msgpack.spec.MsgPackWriter;
import io.zeebe.msgpack.value.BaseValue;
import io.zeebe.msgpack.value.StringValue;
import java.util.Objects;

public abstract class BaseProperty<T extends BaseValue> implements Recyclable {
  protected StringValue key;
  protected T value;
  protected T defaultValue;
  protected boolean isSet;

  public BaseProperty(T value) {
    this(StringValue.EMPTY_STRING, value);
  }

  public BaseProperty(String keyString, T value) {
    this(keyString, value, null);
  }

  public BaseProperty(String keyString, T value, T defaultValue) {
    Objects.requireNonNull(keyString);
    Objects.requireNonNull(value);

    this.key = new StringValue(keyString);
    this.value = value;
    this.defaultValue = defaultValue;
  }

  public void set() {
    this.isSet = true;
  }

  @Override
  public void reset() {
    this.isSet = false;
    this.value.reset();
  }

  public boolean hasValue() {
    return isSet || defaultValue != null;
  }

  public StringValue getKey() {
    return key;
  }

  protected T resolveValue() {
    if (isSet) {
      return value;
    } else if (defaultValue != null) {
      return defaultValue;
    } else {
      throw new MsgpackPropertyException(
          key, "Expected a value or default value to be specified, but has nothing");
    }
  }

  public int getEncodedLength() {
    return key.getEncodedLength() + resolveValue().getEncodedLength();
  }

  public void read(MsgPackReader reader) {
    value.read(reader);
    set();
  }

  public void write(MsgPackWriter writer) {
    T valueToWrite = value;
    if (!isSet) {
      valueToWrite = defaultValue;
    }

    if (valueToWrite == null) {
      throw new MsgpackPropertyException(
          key, "Expected a value or default value to be set before writing, but has nothing");
    }

    key.write(writer);
    valueToWrite.write(writer);
  }

  public void writeJSON(StringBuilder sb) {
    key.writeJSON(sb);
    sb.append(":");
    if (hasValue()) {
      resolveValue().writeJSON(sb);
    } else {
      sb.append("\"NO VALID WRITEABLE VALUE\"");
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey(), value, defaultValue, isSet);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof BaseProperty)) {
      return false;
    }

    final BaseProperty<?> that = (BaseProperty<?>) o;
    return Objects.equals(getKey(), that.getKey())
        && Objects.equals(resolveValue(), that.resolveValue());
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(key.toString());
    builder.append(" => ");
    builder.append(value.toString());
    return builder.toString();
  }
}
