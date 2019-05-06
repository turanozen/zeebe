/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.job;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.CommandProcessor;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.util.buffer.BufferUtil;

public class CreateProcessor implements CommandProcessor<JobRecord> {

  private final JobState state;
  private final ClusterCommunicationService communicationService;
  private final int partitionId;

  public CreateProcessor(
      JobState state, ClusterCommunicationService communicationService, int partitionId) {
    this.state = state;
    this.communicationService = communicationService;
    this.partitionId = partitionId;
  }

  @Override
  public void onCommand(TypedRecord<JobRecord> command, CommandControl<JobRecord> commandControl) {
    final long key = commandControl.accept(JobIntent.CREATED, command.getValue());
    state.create(key, command.getValue());

    final String jobType = BufferUtil.bufferAsString(command.getValue().getType());
    final String subject = "jobs-" + jobType;

    Loggers.STREAM_PROCESSING.info("broadcast to " + subject);

    communicationService.broadcastIncludeSelf(subject, partitionId);
  }
}
