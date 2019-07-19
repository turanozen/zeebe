/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.transport.impl;

import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.RemoteAddressList;
import io.zeebe.transport.SocketAddress;
import java.util.function.Consumer;

/** Threadsafe datastructure for indexing remote addresses and assigning streamIds. */
public class RemoteAddressListImpl implements RemoteAddressList {
  private volatile int size;
  private RemoteAddressImpl[] index = new RemoteAddressImpl[0];

  private Consumer<RemoteAddressImpl> onAddressAddedConsumer = r -> {};

  @Override
  public synchronized RemoteAddressImpl getByStreamId(int streamId) {
    if (streamId < size) {
      return index[streamId];
    }

    return null;
  }

  /**
   * Returns only active addresses
   *
   * @param inetSocketAddress
   * @return
   */
  @Override
  public RemoteAddressImpl getByAddress(SocketAddress inetSocketAddress) {
    return getByAddress(inetSocketAddress, RemoteAddressImpl.STATE_ACTIVE);
  }

  /**
   * Effect: This remote address/stream is never used again; no channel will every be managed for
   * this again; if the underlying socket address is registered again, a new remote address is
   * assigned (+ new stream id)
   */
  @Override
  public synchronized void retire(RemoteAddress remote) {
    getByStreamId(remote.getStreamId()).retire();
  }

  /**
   * This stream is deactivated until it is registered again; the stream id in this case will remain
   * stable
   *
   * @param remote
   */
  @Override
  public synchronized void deactivate(RemoteAddress remote) {
    getByStreamId(remote.getStreamId()).deactivate();
  }

  @Override
  public synchronized void deactivateAll() {
    for (int i = 0; i < size; i++) {
      index[i].deactivate();
    }
  }

  @Override
  public RemoteAddressImpl register(SocketAddress inetSocketAddress) {
    RemoteAddressImpl result = getByAddress(inetSocketAddress);

    if (result == null) {
      synchronized (this) {
        result =
            getByAddress(
                inetSocketAddress,
                RemoteAddressImpl.STATE_ACTIVE | RemoteAddressImpl.STATE_INACTIVE);

        if (result == null) {
          final int prevSize = size;
          final int newSize = prevSize + 1;

          final RemoteAddressImpl remoteAddress =
              new RemoteAddressImpl(prevSize, new SocketAddress(inetSocketAddress));

          final RemoteAddressImpl[] newAddresses = new RemoteAddressImpl[newSize];
          System.arraycopy(index, 0, newAddresses, 0, prevSize);
          newAddresses[remoteAddress.getStreamId()] = remoteAddress;

          this.index = newAddresses;
          this.size = newSize; // publish

          result = remoteAddress;
        } else {
          if (result.isInAnyState(RemoteAddressImpl.STATE_INACTIVE)) {
            result.activate();
          }
        }

        onAddressAddedConsumer.accept(result);
      }
    }

    return result;
  }

  private synchronized RemoteAddressImpl getByAddress(
      SocketAddress inetSocketAddress, int stateMask) {
    final int currSize = size;

    for (int i = 0; i < currSize; i++) {
      final RemoteAddressImpl remoteAddress = index[i];

      if (remoteAddress != null) {
        if (remoteAddress.getAddress().equals(inetSocketAddress)
            && remoteAddress.isInAnyState(stateMask)) {
          return remoteAddress;
        }
      }
    }

    return null;
  }

  public synchronized void setOnAddressAddedConsumer(
      Consumer<RemoteAddressImpl> onAddressAddedConsumer) {
    this.onAddressAddedConsumer = onAddressAddedConsumer;
  }
}
