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

import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;

import java.util.Objects;

/**
 * Used to gather gRPC level resources and indexes together.
 */
public class GrpcConnection implements AutoCloseable {

  private GrpcConnectionKey mKey;
  private ManagedChannel mManagedChannel;
  private Channel mChannel;
  private AlluxioConfiguration mConfiguration;

  /**
   * Creates a new connection object.
   *
   * @param key gRPC channel key
   * @param managedChannel the underlying gRPC {@link ManagedChannel}
   * @param conf the Alluxio configuration
   */
  public GrpcConnection(GrpcConnectionKey key, ManagedChannel managedChannel,
      AlluxioConfiguration conf) {
    mConfiguration = conf;
    mKey = key;
    mManagedChannel = managedChannel;
    mChannel = mManagedChannel;
  }

  /**
   * Gets the gRPC channel key for this connection.
   * <p>
   * Returns the existing {@link #mKey} for this
   * {@link GrpcConnection}.
   *
   * @return  the channel key that owns this gRPC
   *          connection
   */
  public GrpcChannelKey getChannelKey() {
    return mKey.getChannelKey();
  }

  /**
   * Returns the RPC channel.
   * <p>
   * Returns the existing {@link #mChannel}, a virtual connection
   * to a conceptual endpoint, to perform RPCs.
   *
   * @return the RPC channel
   */
  public Channel getChannel() {
    return mChannel;
  }

  /**
   * Registers interceptor to the channel.
   *
   * @param interceptor the gRPC client interceptor
   */
  public void interceptChannel(ClientInterceptor interceptor) {
    mChannel = ClientInterceptors.intercept(mChannel, interceptor);
  }

  /**
   * @return the configuration used when the connection was established
   */
  public AlluxioConfiguration getConfiguration() {
    return mConfiguration;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof GrpcConnection)) {
      return false;
    }
    GrpcConnection otherConnection = (GrpcConnection) other;
    return Objects.equals(mKey, otherConnection.mKey)
        && Objects.equals(mManagedChannel, otherConnection.mManagedChannel)
        && Objects.equals(mChannel, ((GrpcConnection) other).mChannel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mKey, mManagedChannel, mChannel);
  }

  /**
   * Releases the connection to the pool.
   *
   * @throws Exception not expected
   */
  @Override
  public void close() throws Exception {
    // Release the connection back.
    GrpcConnectionPool.INSTANCE.releaseConnection(mKey, mConfiguration);
  }
}
