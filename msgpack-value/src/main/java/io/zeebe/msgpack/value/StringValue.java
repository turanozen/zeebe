/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.msgpack.value;

import static io.zeebe.util.buffer.BufferUtil.wrapString;

import io.zeebe.msgpack.spec.MsgPackReader;
import io.zeebe.msgpack.spec.MsgPackWriter;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class StringValue extends BaseValue {
  public static final String EMPTY_STRING = "";

  private MutableDirectBuffer bytes = new UnsafeBuffer(0, 0);
  private int length;
  private int hashCode;

  public StringValue() {
    this(EMPTY_STRING);
  }

  public StringValue(String string) {
    this(wrapString(string));
  }

  public StringValue(DirectBuffer buffer) {
    this(buffer, 0, buffer.capacity());
  }

  public StringValue(DirectBuffer buffer, int offset, int length) {
    wrap(buffer, offset, length);
  }

  @Override
  public void reset() {
    bytes.wrap(0, 0);
    length = 0;
    hashCode = 0;
  }

  public void wrap(byte[] bytes) {
    this.bytes.wrap(bytes);
    this.length = bytes.length;
    this.hashCode = 0;
  }

  public void wrap(DirectBuffer buff) {
    wrap(buff, 0, buff.capacity());
  }

  public void wrap(DirectBuffer buff, int offset, int length) {
    if (length == 0) {
      this.bytes.wrap(0, 0);
    } else {
      this.bytes.wrap(buff, offset, length);
    }
    this.length = length;
    this.hashCode = 0;
  }

  public void wrap(StringValue anotherString) {
    this.wrap(anotherString.getValue());
  }

  public int getLength() {
    return length;
  }

  public DirectBuffer getValue() {
    return bytes;
  }

  @Override
  public void writeJSON(StringBuilder builder) {
    builder.append("\"");
    builder.append(toString());
    builder.append("\"");
  }

  @Override
  public void write(MsgPackWriter writer) {
    writer.writeString(bytes);
  }

  @Override
  public void read(MsgPackReader reader) {
    final DirectBuffer buffer = reader.getBuffer();
    final int stringLength = reader.readStringLength();
    final int offset = reader.getOffset();

    reader.skipBytes(stringLength);

    this.wrap(buffer, offset, stringLength);
  }

  @Override
  public int getEncodedLength() {
    return MsgPackWriter.getEncodedStringLength(length);
  }

  @Override
  public String toString() {
    return bytes.getStringWithoutLengthUtf8(0, length);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bytes, getLength());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof StringValue)) {
      return false;
    }

    final StringValue that = (StringValue) o;
    return getLength() == that.getLength() && Objects.equals(bytes, that.bytes);
  }
}
