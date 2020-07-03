/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcChannelKey;
import alluxio.grpc.SaslMessage;
import alluxio.util.LogUtils;

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Responsible for driving authentication traffic from client-side.
 *
 * An authentication between client and server is managed by
 * {@link AuthenticatedChannelClientDriver} and {@link AuthenticatedChannelServerDriver}
 * respectively.
 *
 * These drivers are wrappers over gRPC {@link StreamObserver}s that manages the stream
 * traffic destined for the other participant. They make sure messages are exchanged between client
 * and server synchronously.
 *
 * Authentication is initiated by the client. Following the initiate call, depending on the scheme,
 * one or more messages are exchanged to establish authenticated session between client and server.
 *
 * After the authentication is established, client and server streams are not closed in order to use
 * them as long polling on authentication state changes.
 *  -> Client closing the stream means that it doesn't want to be authenticated anymore.
 *  -> Server closing the stream means the client is not authenticated at the server anymore.
 *
 */
public class AuthenticatedChannelClientDriver implements StreamObserver<SaslMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticatedChannelClientDriver.class);
  /** Channel key. */
  private GrpcChannelKey mChannelKey;
  /** Server's sasl stream. */
  private StreamObserver<SaslMessage> mRequestObserver;
  /** Handshake handler for client. */
  private SaslClientHandler mSaslClientHandler;
  /** Whether channel is authenticated. */
  private volatile boolean mChannelAuthenticated;
  /** Used to wait during authentication handshake. */
  private SettableFuture<Void> mChannelAuthenticatedFuture;
  /** Initiating message for authentication. */
  private SaslMessage mInitiateMessage;

  /**
   * Creates client driver with given handshake handler.
   *
   * @param saslClientHandler sasl client handler
   * @param channelKey channel key
   */
  public AuthenticatedChannelClientDriver(SaslClientHandler saslClientHandler,
      GrpcChannelKey channelKey) throws SaslException {
    mSaslClientHandler = saslClientHandler;
    mChannelKey = channelKey;
    mChannelAuthenticated = false;
    mChannelAuthenticatedFuture = SettableFuture.create();
    // Generate the initiating message while sasl handler is valid.
    mInitiateMessage = generateInitialMessage();
  }

  /**
   * Sets the server's Sasl stream.
   *
   * @param requestObserver server Sasl stream
   */
  public void setServerObserver(StreamObserver<SaslMessage> requestObserver) {
    mRequestObserver = requestObserver;
  }

  /**
   * Attempts to establish a connection to a channel.
   * <p>
   * Establishes a connection to the {@link alluxio.grpc.GrpcChannel}
   * with the corresponding {@link GrpcChannelKey} using a handshake handler
   * for the client, that is, {@link SaslClientHandler#handleMessage(SaslMessage)}.
   *
   * @param saslMessage Simple Authentication and Security Layer message
   * @throws Exception  Any exception.
   */
  @Override
  public void onNext(SaslMessage saslMessage) {
    try {
      LOG.debug("Received message for channel: {}. Message: {}",
          mChannelKey.toStringShort(), saslMessage);
      SaslMessage response = mSaslClientHandler.handleMessage(saslMessage);
      if (response != null) {
        mRequestObserver.onNext(response);
      } else {
        // {@code null} response means server message was a success.
        // Release blocked waiters.
        LOG.debug("Authentication established for {}", mChannelKey.toStringShort());
        mChannelAuthenticatedFuture.set(null);
      }
    } catch (Throwable t) {
      LOG.debug("Exception while handling message for {}. Message: {}. Error: {}",
          mChannelKey.toStringShort(), saslMessage, t);
      // Fail blocked waiters.
      mChannelAuthenticatedFuture.setException(t);
      mRequestObserver.onError(AlluxioStatusException.fromThrowable(t).toGrpcStatusException());
    }
  }

  @Override
  public void onError(Throwable throwable) {
    LOG.debug("Received error for channel: {}. Error: {}", mChannelKey.toStringShort(), throwable);
    closeAuthenticatedChannel(false);

    // Fail blocked waiters.
    mChannelAuthenticatedFuture.setException(throwable);
  }

  @Override
  public void onCompleted() {
    LOG.debug("Authenticated channel revoked by server for channel: {}",
        mChannelKey.toStringShort());
    closeAuthenticatedChannel(false);
  }

  /**
   * Stops authenticated session with the server by releasing the long poll.
   */
  public void close() {
    LOG.debug("Closing authentication for channel: {}", mChannelKey.toStringShort());
    closeAuthenticatedChannel(true);
  }

  /**
   * @return {@code true} if the channel is still authenticated
   */
  public boolean isAuthenticated() {
    return mChannelAuthenticated;
  }

  /**
   * Starts authentication with the server and wait until completion.
   *
   * @param timeoutMs time to wait for authentication
   * @throws UnauthenticatedException
   */
  public void startAuthenticatedChannel(long timeoutMs) throws AlluxioStatusException {
    try {
      LOG.debug("Initiating authentication for channel: {}", mChannelKey.toStringShort());
      // Send the server initial message.
      try {
        mRequestObserver.onNext(mInitiateMessage);
      } catch (StatusRuntimeException e) {
        // Ignore. waitUntilChannelAuthenticated() will throw stored cause.
      }

      // Utility to return from start when channel is secured.
      waitUntilChannelAuthenticated(timeoutMs);
    } catch (Throwable t) {
      closeAuthenticatedChannel(true);
      throw AlluxioStatusException.fromThrowable(t);
    }
  }

  /**
   * Builds and returns a SASL message.
   * <p>
   * Uses {@link SaslMessage.Builder#build()} to instantiate a new
   * object of type SaslMessage. The client ID is set to a string of
   * the ID found in {@link AuthenticatedChannelClientDriver#mChannelKey}
   * through {@link GrpcChannelKey#getChannelId()}.
   * <p>
   * The channel ref is set to a short representation of the channel key through
   * {@link GrpcChannelKey#toStringShort()}.
   * <p>
   * Returns an object of type SaslMessage.
   *
   * @return a message for Simple Authentication and Security Layer
   * @throws SaslException  If the message cannot be generated for
   *                        unexpected reasons.
   */
  private SaslMessage generateInitialMessage() throws SaslException {
    SaslMessage.Builder initialMsg = mSaslClientHandler.handleMessage(null).toBuilder();
    initialMsg.setClientId(mChannelKey.getChannelId().toString());
    initialMsg.setChannelRef(mChannelKey.toStringShort());
    return initialMsg.build();
  }

  /**
   * Sets a time limit for the duration of the channel authentication and waits until completion or timeout.
   * <p>
   * Uses {@link AuthenticatedChannelClientDriver#mChannelAuthenticatedFuture} to implement a time limit to
   * how long the authentication for this channel is allowed to take. For this purpose, sets
   * {@link AuthenticatedChannelClientDriver#mChannelAuthenticated} to true.
   *
   * @param timeoutMs the max duration of the wait time for the channel
   *                  authentication in milliseconds. Throws an exception
   *                  if the authentication fails to complete until this point.
   * @throws AlluxioStatusException   If this thread is interrupted.
   * @throws UnauthenticatedException If the server does not provide
   *                                  an authentication service.
   * @throws UnavailableException     If a {@link TimeoutException}
   *                                  is thrown, indicating that
   *                                  the authentication service
   *                                  took longer than the max
   *                                  duration established.
   */
  private void waitUntilChannelAuthenticated(long timeoutMs) throws AlluxioStatusException {
    try {
      // Wait until authentication status changes.
      mChannelAuthenticatedFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
      mChannelAuthenticated = true;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw AlluxioStatusException.fromThrowable(ie);
    } catch (ExecutionException e) {
      AlluxioStatusException statExc = AlluxioStatusException.fromThrowable(e.getCause());
      // Unimplemented is returned if server doesn't provide authentication service.
      if (statExc.getStatusCode() == Status.Code.UNIMPLEMENTED) {
        throw new UnauthenticatedException("Authentication is disabled on target server.");
      }
      throw statExc;
    } catch (TimeoutException e) {
      throw new UnavailableException(e);
    }
  }

  /**
   * Closes the Simple Authentication and Security Layer (SASL) client handler.
   * <p>
   * Closes the {@link #mSaslClientHandler} and sets
   * {@link #mChannelAuthenticated} to false, which
   * indicates whether this channel is authenticated.
   * <p>
   * Attempts to notify {@link #mRequestObserver} that the
   * stream was successfully completed if {@code signalServer}
   * is set to true.
   *
   * @param signalServer  a boolean indicating whether
   *                      the server should be signaled
   */
  private void closeAuthenticatedChannel(boolean signalServer) {
    mSaslClientHandler.close();
    // Authentication failed either during or after handshake.
    mChannelAuthenticated = false;

    if (signalServer) {
      try {
        mRequestObserver.onCompleted();
      } catch (Exception e) {
        LogUtils.warnWithException(LOG,
            "Failed signaling server for stream completion for channel: {}.",
            mChannelKey.toStringShort(), e);
      }
    }
  }
}
