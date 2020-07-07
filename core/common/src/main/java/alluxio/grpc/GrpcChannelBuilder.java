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

package alluxio.grpc;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedChannelClientDriver;
import alluxio.security.authentication.ChannelAuthenticator;

import javax.security.auth.Subject;

/**
 * A gRPC channel builder that authenticates with {@link GrpcServer} at the target during channel
 * building.
 */
public final class GrpcChannelBuilder {
  /** gRPC channel key. */
  private final GrpcChannelKey mChannelKey;

  /** Subject for authentication. */
  private Subject mParentSubject;

  /** Whether to authenticate the channel with the server. */
  private boolean mAuthenticateChannel;

  // Configuration constants.
  private final AuthType mAuthType;

  private AlluxioConfiguration mConfiguration;

  private GrpcChannelBuilder(GrpcServerAddress address, AlluxioConfiguration conf) {
    mConfiguration = conf;

    // Read constants.
    mAuthType =
        mConfiguration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);

    // Set default overrides for the channel.
    mChannelKey = GrpcChannelKey.create(conf);
    mChannelKey.setServerAddress(address);

    // Determine if authentication required.
    mAuthenticateChannel = mAuthType != AuthType.NOSASL;
  }

  /**
   * Create a channel builder for given address using the given configuration.
   *
   * @param address the host address
   * @param conf Alluxio configuration
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public static GrpcChannelBuilder newBuilder(GrpcServerAddress address,
      AlluxioConfiguration conf) {
    return new GrpcChannelBuilder(address, conf);
  }

  /**
   * Sets human readable name for the channel's client.
   * <p>
   * Invokes {@link GrpcChannelBuilder#mChannelKey#setClientType}
   * to set the client type to the one corresponding to the string
   * provided to represent the client type.
   * <p>
   * Returns this gRPC channel builder once it is updated.
   *
   * @param clientType  the client type
   * @return            the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder setClientType(String clientType) {
    mChannelKey.setClientType(clientType);
    return this;
  }

  /**
   * Sets a given subject for authentication.
   * <p>
   * Sets the {@link #mParentSubject} of this gRPC channel builder
   * to the provided {@code subject}. Updates and returns this
   * instance of {@link GrpcChannelBuilder}.
   * <p>
   * Sets {@link Subject} for authentication.
   *
   * @param subject the subject
   * @return        the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder setSubject(Subject subject) {
    mParentSubject = subject;
    return this;
  }

  /**
   * Disables authentication with the server.
   *
   * @return the updated {@link GrpcChannelBuilder} instance
   */
  public GrpcChannelBuilder disableAuthentication() {
    mAuthenticateChannel = false;
    return this;
  }

  /**
   * Sets the pooling strategy.
   *
   * @param group the networking group
   * @return a new instance of {@link GrpcChannelBuilder}
   */
  public GrpcChannelBuilder setNetworkGroup(GrpcNetworkGroup group) {
    mChannelKey.setNetworkGroup(group);
    return this;
  }

  /**
   * Creates and returns a new gRPC channel.
   * <p>
   * Creates a new {@link GrpcConnection} using:
   *          1) the {@link GrpcChannelBuilder#mChannelKey};
   *          2) the {@link GrpcChannelBuilder#mConfiguration}.
   * <p>
   * Checks whether the {@link GrpcChannelBuilder#mAuthenticateChannel} is
   * set to true, in which case the channel should be authenticated with the
   * server. The authentication is made through an object of type
   * {@link ChannelAuthenticator} instantiated using:
   *          1) the object of type GrpcConnection that was created previously;
   *          2) the {@link GrpcChannelBuilder#mParentSubject} indicating the javax
   *          subject to use for authentication;
   *          3) the {@link GrpcChannelBuilder#mAuthType} indicating the requested
   *          authentication type.
   *          4) the {@link GrpcChannelBuilder#mConfiguration} indicating the Alluxio
   *          configuration.
   * The new instance of ChannelAuthenticator invokes {@link ChannelAuthenticator#authenticate}
   * in order to authenticate a new logical channel. An {@link AuthenticatedChannelClientDriver}
   * is created using {@link ChannelAuthenticator#getAuthenticationDriver()} from the previously
   * created channel authenticator.
   * <p>
   * If the channel should not be authenticated with the server, no channel authenticator is created
   * and the authenticated channel client driver remains as null.
   *
   * Returns a wrapper over the logical channel.
   *
   * @return  a new gRPC channel
   * @throws  RuntimeException        If the connection cannot be released.
   * @throws  UnavailableException    If the target channel is unavailable.
   * @throws  AlluxioStatusException  If any other unforeseen exception is caught while
   *                                  trying to build a new gRPC channel.
   */
  public GrpcChannel build() throws AlluxioStatusException {
    // Acquire a connection from the pool.
    GrpcConnection connection =
        GrpcConnectionPool.INSTANCE.acquireConnection(mChannelKey, mConfiguration);
    try {
      AuthenticatedChannelClientDriver authDriver = null;
      if (mAuthenticateChannel) {
        // Create channel authenticator based on provided content.
        ChannelAuthenticator channelAuthenticator = new ChannelAuthenticator(
            connection, mParentSubject, mAuthType, mConfiguration);
        // Authenticate a new logical channel.
        channelAuthenticator.authenticate();
        // Acquire authentication driver.
        authDriver = channelAuthenticator.getAuthenticationDriver();
      }
      // Return a wrapper over logical channel.
      return new GrpcChannel(connection, authDriver);
    } catch (Throwable t) {
      try {
        connection.close();
      } catch (Exception e) {
        throw new RuntimeException("Failed release the connection.", e);
      }
      // Pretty print unavailable cases.
      if (t instanceof UnavailableException) {
        throw new UnavailableException(
            String.format("Target Unavailable. %s", mChannelKey.toStringShort()), t.getCause());
      }
      throw AlluxioStatusException.fromThrowable(t);
    }
  }
}
