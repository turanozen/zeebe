/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.util.sched.testing;

import io.zeebe.util.LangUtil;
import io.zeebe.util.sched.ActorThread;
import io.zeebe.util.sched.ActorThreadGroup;
import io.zeebe.util.sched.ActorTimerQueue;
import io.zeebe.util.sched.TaskScheduler;
import io.zeebe.util.sched.clock.ActorClock;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ControlledActorThread extends ActorThread {
  private CyclicBarrier barrier = new CyclicBarrier(2);

  public ControlledActorThread(
      String name,
      int id,
      ActorThreadGroup threadGroup,
      TaskScheduler taskScheduler,
      ActorClock clock,
      ActorTimerQueue timerQueue) {
    super(name, id, threadGroup, taskScheduler, clock, timerQueue);
    idleStrategy = new ControlledIdleStartegy();
  }

  public void workUntilDone() {
    try {
      barrier.await(); // work at least 1 full cycle until the runner becomes idle after having been
      while (barrier.getNumberWaiting() < 1) {
        // spin until thread is idle again
        Thread.yield();
      }
    } catch (InterruptedException | BrokenBarrierException e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  class ControlledIdleStartegy extends ActorTaskRunnerIdleStrategy {
    @Override
    protected void onIdle() {
      super.onIdle();

      try {
        barrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        LangUtil.rethrowUnchecked(e);
      }
    }
  }
}
