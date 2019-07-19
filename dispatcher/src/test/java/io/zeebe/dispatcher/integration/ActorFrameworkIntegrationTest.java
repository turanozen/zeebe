/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.dispatcher.integration;

import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.ClaimedFragment;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.Dispatchers;
import io.zeebe.dispatcher.FragmentHandler;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.util.ByteValue;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;

public class ActorFrameworkIntegrationTest {
  @Rule public ActorSchedulerRule actorSchedulerRule = new ActorSchedulerRule(3);

  @Test
  public void testOffer() throws InterruptedException {
    final Dispatcher dispatcher =
        Dispatchers.create("default")
            .actorScheduler(actorSchedulerRule.get())
            .bufferSize(ByteValue.ofMegabytes(10))
            .build();

    actorSchedulerRule.submitActor(new Consumer(dispatcher));
    final Producer producer = new Producer(dispatcher);
    actorSchedulerRule.submitActor(producer);

    producer.latch.await();
    dispatcher.close();
  }

  @Test
  public void testClaim() throws InterruptedException {
    final Dispatcher dispatcher =
        Dispatchers.create("default")
            .actorScheduler(actorSchedulerRule.get())
            .bufferSize(ByteValue.ofMegabytes(10))
            .build();

    actorSchedulerRule.submitActor(new Consumer(dispatcher));
    final ClaimingProducer producer = new ClaimingProducer(dispatcher);
    actorSchedulerRule.submitActor(producer);

    producer.latch.await();
    dispatcher.close();
  }

  @Test
  public void testClaimAndPeek() throws InterruptedException {
    final Dispatcher dispatcher =
        Dispatchers.create("default")
            .actorScheduler(actorSchedulerRule.get())
            .bufferSize(ByteValue.ofMegabytes(10))
            .build();

    actorSchedulerRule.submitActor(new PeekingConsumer(dispatcher));
    final ClaimingProducer producer = new ClaimingProducer(dispatcher);
    actorSchedulerRule.submitActor(producer);

    producer.latch.await();
    dispatcher.close();
  }

  class Consumer extends Actor implements FragmentHandler {
    final Dispatcher dispatcher;
    Subscription subscription;
    int counter = 0;

    Consumer(Dispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    protected void onActorStarted() {
      final ActorFuture<Subscription> future =
          dispatcher.openSubscriptionAsync("consumerSubscription-" + hashCode());
      actor.runOnCompletion(
          future,
          (s, t) -> {
            this.subscription = s;
            actor.consume(subscription, this::consume);
          });
    }

    void consume() {
      subscription.poll(this, Integer.MAX_VALUE);
    }

    @Override
    public int onFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int streamId,
        boolean isMarkedFailed) {
      final int newCounter = buffer.getInt(offset);
      if (newCounter - 1 != counter) {
        throw new RuntimeException(newCounter + " " + counter);
      }
      counter = newCounter;
      return FragmentHandler.CONSUME_FRAGMENT_RESULT;
    }
  }

  class PeekingConsumer extends Actor implements FragmentHandler {
    final Dispatcher dispatcher;
    final BlockPeek peek = new BlockPeek();
    Subscription subscription;
    int counter = 0;
    final Runnable processPeek = this::processPeek;

    PeekingConsumer(Dispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    protected void onActorStarted() {
      final ActorFuture<Subscription> future =
          dispatcher.openSubscriptionAsync("consumerSubscription-" + hashCode());
      actor.runOnCompletion(
          future,
          (s, t) -> {
            this.subscription = s;
            actor.consume(subscription, this::consume);
          });
    }

    void consume() {
      if (subscription.peekBlock(peek, Integer.MAX_VALUE, true) > 0) {
        actor.runUntilDone(processPeek);
      }
    }

    void processPeek() {
      final Iterator<DirectBuffer> iterator = peek.iterator();
      while (iterator.hasNext()) {
        final DirectBuffer directBuffer = iterator.next();
        final int newCounter = directBuffer.getInt(0);
        if (newCounter - 1 != counter) {
          throw new RuntimeException(newCounter + " " + counter);
        }
        counter = newCounter;
      }
      peek.markCompleted();
      actor.done();
    }

    @Override
    public int onFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int streamId,
        boolean isMarkedFailed) {
      final int newCounter = buffer.getInt(offset);
      if (newCounter - 1 != counter) {
        throw new RuntimeException(newCounter + " " + counter);
      }
      counter = newCounter;
      return FragmentHandler.CONSUME_FRAGMENT_RESULT;
    }
  }

  class Producer extends Actor {
    final CountDownLatch latch = new CountDownLatch(1);

    final int totalWork = 10_000;
    final UnsafeBuffer msg = new UnsafeBuffer(ByteBuffer.allocate(4534));

    final Dispatcher dispatcher;
    int counter = 1;

    Runnable produce = this::produce;

    Producer(Dispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    protected void onActorStarted() {
      actor.run(produce);
    }

    void produce() {
      msg.putInt(0, counter);

      if (dispatcher.offer(msg) >= 0) {
        counter++;
      }

      if (counter < totalWork) {
        actor.yield();
        actor.run(produce);
      } else {
        latch.countDown();
      }
    }
  }

  class ClaimingProducer extends Actor {
    final CountDownLatch latch = new CountDownLatch(1);

    final int totalWork = 10_000;

    final Dispatcher dispatcher;
    final ClaimedFragment claim = new ClaimedFragment();
    int counter = 1;
    Runnable produce = this::produce;

    ClaimingProducer(Dispatcher dispatcher) {
      this.dispatcher = dispatcher;
    }

    @Override
    protected void onActorStarted() {
      actor.run(produce);
    }

    void produce() {
      if (dispatcher.claim(claim, 4534) >= 0) {
        claim.getBuffer().putInt(claim.getOffset(), counter++);
        claim.commit();
      }

      if (counter < totalWork) {
        actor.yield();
        actor.run(produce);
      } else {
        latch.countDown();
      }
    }
  }
}
