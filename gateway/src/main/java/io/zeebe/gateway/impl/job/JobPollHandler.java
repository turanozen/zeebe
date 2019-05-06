package io.zeebe.gateway.impl.job;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.Loggers;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ScheduledTimer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntConsumer;

public class JobPollHandler extends Actor {

  private static final Logger LOG = Loggers.GATEWAY_LOGGER;

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private final Map<String, List<StreamObserver<?>>> pendingRequest = new HashMap<>();

  private final ClusterCommunicationService communicationService;

  public JobPollHandler(ClusterCommunicationService communicationService) {
    this.communicationService = communicationService;
  }

  @Override
  public String getName() {
    return "gateway-job-poll-handler";
  }

  public void register(
      String jobType, StreamObserver<?> observer, IntConsumer callback, Duration timeout) {
    actor.call(() -> register_(jobType, observer, callback, timeout));
  }

  private void register_(
      String jobType, StreamObserver<?> observer, IntConsumer callback, Duration timeout) {

    // schedule timeout if no jobs are available
    final ScheduledTimer timeoutCallback =
        actor.runDelayed(
            timeout,
            () -> {

              // un-subscribe if there are no other pending requests
              final List<StreamObserver<?>> requests = pendingRequest.get(jobType);

              final boolean isRemoved = requests.remove(observer);

              // close request
              if (isRemoved) {
                LOG.info("timeout - closing request");

                observer.onCompleted();

                if (requests.isEmpty()) {
                  unsubscribe(jobType);
                }
              }
            });

    // subscribe if not yet done
    final List<StreamObserver<?>> requests =
        pendingRequest.computeIfAbsent(jobType, k -> new ArrayList<>());

    if (requests.isEmpty()) {
      subscribe(
          jobType,
          partitionId -> {

            // unsubscribe
            unsubscribe(jobType);

            LOG.info("new job available at partition: " + partitionId);

            final boolean isRemoved = requests.remove(observer);

            if (isRemoved) {
              // poll new jobs
              callback.accept(partitionId);
            }

            // cancel timeout callback
            timeoutCallback.cancel();

            // close other requests to trigger new polling
            requests.forEach(
                request -> {
                  LOG.info("close request after jobs are polled by another request");
                  request.onCompleted();
                });

            requests.clear();
          });
    }

    requests.add(observer);
  }

  private void subscribe(String jobType, IntConsumer callback) {

    final String subject = "jobs-" + jobType;
    LOG.info("subscribe to " + subject);

    communicationService.subscribe(
        subject,
        (Integer partitionId) -> {
          actor.call(() -> callback.accept(partitionId));
          return;
        },
        executorService);
  }

  private void unsubscribe(String jobType) {

    final String subject = "jobs-" + jobType;

    LOG.info("unsubscribe from " + subject);
    communicationService.unsubscribe(subject);
  }

  @Override
  protected void onActorClosing() {

    LOG.info("Closing all pending requests");
    pendingRequest.values().stream().flatMap(l -> l.stream()).forEach(StreamObserver::onCompleted);

    executorService.shutdown();
  }
}
