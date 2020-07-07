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
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.MoreObjects;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.UUID;

/**
 * Used to identify a unique {@link GrpcChannel}.
 */
public class GrpcChannelKey {
  @IdentityField
  GrpcNetworkGroup mNetworkGroup = GrpcNetworkGroup.RPC;
  @IdentityField
  private GrpcServerAddress mServerAddress;

  /** Unique channel identifier. */
  private final UUID mChannelId = UUID.randomUUID();
  /** Hostname to send to server for identification. */
  private final String mLocalHostName;

  /** Client that requires a channel. */
  private String mClientType;

  private GrpcChannelKey(AlluxioConfiguration conf) {
    // Try to get local host name.
    String localHostName;
    try {
      localHostName = NetworkAddressUtils
          .getLocalHostName((int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    } catch (Exception e) {
      localHostName = NetworkAddressUtils.UNKNOWN_HOSTNAME;
    }
    mLocalHostName = localHostName;
  }

  /**
   * Creates a unique identifier for a gRPC channel.
   * <p>
   * Identifies the {@link alluxio.grpc.GrpcChannel} using a
   * {@link GrpcChannelKey} in an unique and unambiguous way.
   *
   * @param conf  the Alluxio configuration
   * @return      an instance of type GrpcChannelKey
   *              with the specified configurations
   */
  public static GrpcChannelKey create(AlluxioConfiguration conf) {
    return new GrpcChannelKey(conf);
  }

  /**
   * Returns the unique identifier of the channel
   * <p>
   * Gets randomly generated UUID for this channel.
   *
   * @return the UUID for the channel
   */
  public UUID getChannelId() {
    return mChannelId;
  }

  /**
   * Returns the destination address of the gRPC channel.
   * @return destination address of the channel
   */
  public GrpcServerAddress getServerAddress() {
    return mServerAddress;
  }

  /**
   * Sets the address for the gRPC server and returns this instance of GrpcChannelKey.
   * <p>
   * Sets {@link #mServerAddress} to the provided {@link GrpcServerAddress}.
   * <p>
   * Returns this {@link GrpcChannelKey}.
   *
   * @param address destination address of the channel
   * @return        the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setServerAddress(GrpcServerAddress address) {
    mServerAddress = address;
    return this;
  }

  /**
   * @param group the networking group membership
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setNetworkGroup(GrpcNetworkGroup group) {
    mNetworkGroup = group;
    return this;
  }

  /**
   * Gets the network group for this gRPC channel key.
   * <p>
   * Returns the {@link #mNetworkGroup} of this {@link GrpcChannelKey},
   * set by default to {@link GrpcNetworkGroup#RPC}, a networking group
   * for RPC traffic.
   *
   * @return the network group
   */
  public GrpcNetworkGroup getNetworkGroup() {
    return mNetworkGroup;
  }

  /**
   * Sets the client type for this gRPC channel key, updates this object, and returns it.
   * <p>
   * Updates the existing {@link #mClientType} to the provided {@code clientType}.
   *
   * @param   clientType  the new client type
   * @return  the modified {@link GrpcChannelKey}, updated with a new client type
   */
  public GrpcChannelKey setClientType(String clientType) {
    mClientType = clientType;
    return this;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hashCodebuilder = new HashCodeBuilder();
    for (Field field : this.getClass().getDeclaredFields()) {
      if (field.isAnnotationPresent(IdentityField.class)) {
        try {
          hashCodebuilder.append(field.get(this));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(
              String.format("Failed to calculate hashcode for channel-key: %s", this), e);
        }
      }
    }
    return hashCodebuilder.toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof GrpcChannelKey) {
      GrpcChannelKey otherKey = (GrpcChannelKey) other;
      boolean areEqual = true;
      for (Field field : this.getClass().getDeclaredFields()) {
        if (field.isAnnotationPresent(IdentityField.class)) {
          try {
            areEqual &= field.get(this).equals(field.get(otherKey));
          } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format(
                "Failed to calculate equality between channel-keys source: %s | destination: %s",
                this, otherKey), e);
          }
        }
      }
      return areEqual;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ClientType", mClientType)
        .add("ServerAddress", mServerAddress)
        .add("ChannelId", mChannelId)
        .omitNullValues()
        .toString();
  }

  /**
   * Returns a short String representation of this gRPC channel key.
   * <p>
   * Returns a short representation of this channel key, following
   * this template:
   * {@code GrpcChannelKey{ClientType=mClientType, ClientHostname=mLocalHostName, ChannelId=mChannelId}}
   * <p>
   * Null values are omitted.
   *
   * @return short representation of this gRPC channel key
   */
  public String toStringShort() {
    return MoreObjects.toStringHelper(this)
        .add("ClientType", mClientType)
        .add("ClientHostname", mLocalHostName)
        .add("ServerAddress", mServerAddress)
        .add("ChannelId", mChannelId)
        .omitNullValues()
        .toString();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  /**
   * Used to mark fields in this class that are part of
   * the identity of a channel while pooling channels.
   *
   * Values of fields that are marked with this annotation will be used
   * during {@link #hashCode()} and {@link #equals(Object)}.
   */
  protected @interface IdentityField {
  }
}
