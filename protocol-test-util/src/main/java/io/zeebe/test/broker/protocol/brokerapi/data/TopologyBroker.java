/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test.broker.protocol.brokerapi.data;

import io.zeebe.transport.SocketAddress;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class TopologyBroker {
  protected final int nodeId;
  protected final String host;
  protected final int port;
  private Set<BrokerPartitionState> partitions = new LinkedHashSet<>();
  private SocketAddress address;

  public TopologyBroker(final int nodeId, final String host, final int port) {
    this.nodeId = nodeId;
    this.host = host;
    this.port = port;
    address = new SocketAddress(host, port);
  }

  public int getNodeId() {
    return nodeId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public Set<BrokerPartitionState> getPartitions() {
    return partitions;
  }

  public TopologyBroker addPartition(BrokerPartitionState brokerPartitionState) {
    partitions.add(brokerPartitionState);
    return this;
  }

  public SocketAddress getAddress() {
    return address;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TopologyBroker that = (TopologyBroker) o;
    return nodeId == that.nodeId;
  }

  @Override
  public String toString() {
    return "TopologyBroker{"
        + "partitions="
        + partitions
        + ", nodeId="
        + nodeId
        + ", host='"
        + host
        + '\''
        + ", port="
        + port
        + ", address="
        + address
        + '}';
  }
}
