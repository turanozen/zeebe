/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.client.impl;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.SecurityLevel;
import io.grpc.Status;
import io.zeebe.util.AuthConstants;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwtCallCredentials extends CallCredentials {
  private final Metadata headers;
  private final Logger logger;

  JwtCallCredentials(final String jwt) {
    logger = LoggerFactory.getLogger("io.zeebe.jwt");

    headers = new Metadata();
    final Key<String> key = Key.of(AuthConstants.HEADER_AUTH_KEY, Metadata.ASCII_STRING_MARSHALLER);
    headers.put(key, String.format("%s%s", AuthConstants.BEARER_PREFIX, jwt));
  }

  @Override
  public void applyRequestMetadata(
      final RequestInfo requestInfo, final Executor appExecutor, final MetadataApplier applier) {
    if (requestInfo.getSecurityLevel() == SecurityLevel.PRIVACY_AND_INTEGRITY) {
      logger.debug("Applying JWT to call metadata");
      applier.apply(headers);
    } else {
      logger.debug("Failed to apply JWT to call metadata due to unmet security level");
      applier.fail(Status.UNAUTHENTICATED);
    }
  }

  @Override
  public void thisUsesUnstableApi() {}
}
