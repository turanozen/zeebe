/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.servicecontainer.impl;

import io.zeebe.servicecontainer.CompositeServiceBuilder;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceBuilder;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.ServiceInterruptedException;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.channel.ConcurrentQueueChannel;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.slf4j.Logger;

public class ServiceContainerImpl extends Actor implements ServiceContainer {
  public static final Logger LOG = Loggers.SERVICE_CONTAINER_LOGGER;
  private static final String NAME = "service-container-main";
  protected final ServiceDependencyResolver dependencyResolver = new ServiceDependencyResolver();
  protected final ConcurrentQueueChannel<ServiceEvent> channel =
      new ConcurrentQueueChannel<>(new ManyToOneConcurrentLinkedQueue<>());
  protected final ActorScheduler actorScheduler;
  protected final AtomicBoolean isOpenend = new AtomicBoolean(false);
  private final CompletableActorFuture<Void> containerCloseFuture = new CompletableActorFuture<>();
  protected ContainerState state = ContainerState.NEW;

  public ServiceContainerImpl(ActorScheduler scheduler) {
    actorScheduler = scheduler;
  }

  @Override
  public void start() {
    if (isOpenend.compareAndSet(false, true)) {
      actorScheduler.submitActor(this);
      state = ContainerState.OPEN;
    } else {
      final String errorMessage = String.format("Cannot start service container, is already open.");
      throw new IllegalStateException(errorMessage);
    }
  }

  @Override
  public ActorFuture<Boolean> hasService(ServiceName<?> name) {
    return actor.call(() -> hasServiceInternal(name));
  }

  @Override
  public <S> ServiceBuilder<S> createService(ServiceName<S> name, Service<S> service) {
    return new ServiceBuilder<>(name, service, this);
  }

  @Override
  public CompositeServiceBuilder createComposite(ServiceName<Void> name) {
    return new CompositeServiceBuilder(name, this);
  }

  @Override
  public ActorFuture<Void> removeService(ServiceName<?> serviceName) {
    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();

    actor.call(
        () -> {
          if (state == ContainerState.OPEN || state == ContainerState.CLOSING) {
            final ServiceController ctrl = dependencyResolver.getService(serviceName);

            if (ctrl != null) {
              actor.runOnCompletion(
                  ctrl.remove(),
                  (v, t) -> {
                    if (t != null) {
                      future.completeExceptionally(t);
                    } else {
                      future.complete(null);
                    }
                  });
            } else {
              future.complete(null);
            }
          } else {
            final String errorMessage =
                String.format("Cannot remove service, container is '%s'.", state);
            future.completeExceptionally(new IllegalStateException(errorMessage));
          }

          actor.runOnCompletion(
              future,
              (r, t) -> {
                if (t != null) {
                  LOG.error("Failed to remove service {}: {}", serviceName, t);
                }
              });
        });

    return future;
  }

  @Override
  public void close(long awaitTime, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    final ActorFuture<Void> containerCloseFuture = closeAsync();

    try {
      containerCloseFuture.get(awaitTime, timeUnit);
    } catch (Exception ex) {
      LOG.debug("Service container closing failed. Print dependencies.");

      final StringBuilder builder = new StringBuilder();
      dependencyResolver
          .getControllers()
          .forEach(
              (c) -> {
                builder.append("\n").append(c).append("\n\t\\");
                c.getDependencies()
                    .forEach(
                        (d) -> {
                          builder.append("\n \t-- ").append(dependencyResolver.getService(d));
                        });
              });

      LOG.debug(builder.toString());
      throw ex;
    } finally {
      onClosed();
    }
  }

  @Override
  public ActorFuture<Void> closeAsync() {
    actor.call(
        () -> {
          if (state == ContainerState.OPEN) {
            state = ContainerState.CLOSING;

            final List<ActorFuture<Void>> serviceFutures = new ArrayList<>();

            dependencyResolver
                .getRootServices()
                .forEach(
                    c -> {
                      final ActorFuture<Void> removeFuture = c.remove();
                      actor.runOnCompletion(
                          removeFuture,
                          (v, t) -> {
                            LOG.debug("Removed service {}", c.getServiceName());
                          });
                      serviceFutures.add(removeFuture);
                    });

            actor.runOnCompletion(
                serviceFutures,
                (t) -> {
                  actor.close();
                  containerCloseFuture.complete(null);
                });
          } else {
            final String errorMessage =
                String.format("Cannot close service container, container is '%s'.", state);
            containerCloseFuture.completeExceptionally(new IllegalStateException(errorMessage));
          }
        });

    return containerCloseFuture;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  protected void onActorStarted() {
    actor.consume(channel, this::onServiceEvent);
  }

  protected void onServiceEvent() {
    final ServiceEvent serviceEvent = channel.poll();
    if (serviceEvent != null) {
      LOG.trace("{}", serviceEvent);
      dependencyResolver.onServiceEvent(serviceEvent);
    } else {
      actor.yield();
    }
  }

  private boolean hasServiceInternal(ServiceName<?> name) {
    return dependencyResolver.getService(name) != null;
  }

  public <S> ActorFuture<S> onServiceBuilt(ServiceBuilder<S> serviceBuilder) {
    final CompletableActorFuture<S> future = new CompletableActorFuture<>();

    actor.run(
        () -> {
          final ServiceName<?> serviceName = serviceBuilder.getName();
          if (state == ContainerState.OPEN) {
            final ServiceController serviceController =
                new ServiceController(serviceBuilder, this, future);

            if (!hasServiceInternal(serviceController.getServiceName())) {
              actorScheduler.submitActor(serviceController);
            } else {
              final String errorMessage =
                  String.format(
                      "Cannot install service with name '%s'. Service with same name already exists",
                      serviceName);
              future.completeExceptionally(new IllegalStateException(errorMessage));
            }
          } else {
            final String errorMessage =
                String.format(
                    "Cannot install new service %s into the container, state is '%s'",
                    serviceName, state);
            future.completeExceptionally(new IllegalStateException(errorMessage));
          }

          actor.runOnCompletion(
              future,
              (r, t) -> {
                if (t != null) {
                  if (t instanceof ServiceInterruptedException) {
                    LOG.debug(
                        String.format(
                            "Service %s interrupted while building", serviceName.getName()));
                  } else {
                    LOG.error("Failed to build service", t);
                  }
                }
              });
        });

    return future;
  }

  private void onClosed() {
    state = ContainerState.CLOSED;
  }

  public ConcurrentQueueChannel<ServiceEvent> getChannel() {
    return channel;
  }

  public ActorScheduler getActorScheduler() {
    return actorScheduler;
  }

  enum ContainerState {
    NEW,
    OPEN,
    CLOSING,
    CLOSED; // container is not reusable
  }
}
