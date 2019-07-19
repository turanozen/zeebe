/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.instance;

import io.zeebe.db.DbValue;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.BinaryProperty;
import io.zeebe.msgpack.property.LongProperty;
import org.agrona.DirectBuffer;

public class VariableInstance extends UnpackedObject implements DbValue {

  private final LongProperty keyProp = new LongProperty("key");
  private final BinaryProperty valueProp = new BinaryProperty("value");

  public VariableInstance() {
    this.declareProperty(keyProp).declareProperty(valueProp);
  }

  public long getKey() {
    return keyProp.getValue();
  }

  public VariableInstance setKey(long key) {
    keyProp.setValue(key);
    return this;
  }

  public VariableInstance setValue(DirectBuffer value, int offset, int length) {
    valueProp.setValue(value, offset, length);
    return this;
  }

  public DirectBuffer getValue() {
    return valueProp.getValue();
  }
}
