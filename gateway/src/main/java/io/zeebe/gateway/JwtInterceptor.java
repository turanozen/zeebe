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
package io.zeebe.gateway;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.zeebe.util.AuthConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwtInterceptor implements ServerInterceptor {

  private final Key<String> key;
  private final Logger logger;

  public JwtInterceptor() {
    logger = LoggerFactory.getLogger("io.zeebe.jwt");
    key = Key.of(AuthConstants.HEADER_AUTH_KEY, Metadata.ASCII_STRING_MARSHALLER);
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    try {
      final String authValue = headers.get(key);
      if (authValue == null || authValue.isEmpty()) {
        throw new InvalidAuthentication(
            String.format(
                "Expected authorization token (JWT) under key '%s' but found null or empty string instead.",
                AuthConstants.HEADER_AUTH_KEY));
      }

      if (!authValue.startsWith(AuthConstants.BEARER_PREFIX)) {
        throw new InvalidAuthentication(
            String.format(
                "Expected authorization token to start with the '%s' prefix but found '%s' instead.",
                AuthConstants.BEARER_PREFIX, authValue));
      }

      final String encodedJwt = authValue.substring(AuthConstants.BEARER_PREFIX.length());

      final JWTVerifier verifier =
          JWT.require(Algorithm.HMAC256(AuthConstants.TEST_SECRET))
              .withIssuer(AuthConstants.JWT_ISSUER)
              .withSubject(AuthConstants.JWT_SUBJECT)
              .acceptExpiresAt(0)
              .build();

      verifier.verify(encodedJwt);
      logger.info("JWT was verified");

      // NOTE:
      // It's possible to propagate claims to the application through the context. Example:
      //            final Claim clientId = JWT.decode(encodedJwt).getClaim("client-id");
      //            final Context.Key<String> clientIdKey = Context.key("client-id");
      //
      //            final Context context = Context.current().withValue(clientIdKey,
      // clientId.asString());
      //
      //            return Contexts.interceptCall(context, call, headers, next);
      //
      // Then in the server the context's value can be retrieved through `clientIdKey.get();` which
      // is keep somewhere statically

      return next.startCall(call, headers);
    } catch (Exception e) {
      logger.debug("JWT verification failed: ", e);

      call.close(Status.UNAUTHENTICATED.withCause(e), headers);
      return new ServerCall.Listener<ReqT>() {};
    }
  }

  class InvalidAuthentication extends Exception {

    public InvalidAuthentication(final String message) {
      super(message);
    }
  }
}
