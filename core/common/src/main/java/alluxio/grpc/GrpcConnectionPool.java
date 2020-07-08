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
import alluxio.network.ChannelType;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NettyUtils;

import com.google.common.base.Preconditions;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used to provide gRPC level connection management and pooling facilities.
 *
 * This class is used internally by {@link GrpcChannelBuilder} and {@link GrpcChannel}.
 */
@ThreadSafe
public class GrpcConnectionPool {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcConnectionPool.class);

  // Singleton instance.
  public static final GrpcConnectionPool INSTANCE = new GrpcConnectionPool();

  /** gRPC Managed channels/connections. */
  private ConcurrentMap<GrpcConnectionKey, CountingReference<ManagedChannel>> mChannels;

  /** Event loops. */
  private ConcurrentMap<GrpcNetworkGroup, CountingReference<EventLoopGroup>> mEventLoops;

  /** Used to assign order within a network group. */
  private ConcurrentMap<GrpcNetworkGroup, AtomicLong> mNetworkGroupCounters;

  /**
   * Creates a new {@link GrpcConnectionPool}.
   */
  public GrpcConnectionPool() {
    mChannels = new ConcurrentHashMap<>();
    mEventLoops = new ConcurrentHashMap<>();
    // Initialize counters for known network-groups.
    mNetworkGroupCounters = new ConcurrentHashMap<>();
    for (GrpcNetworkGroup group : GrpcNetworkGroup.values()) {
      mNetworkGroupCounters.put(group, new AtomicLong());
    }
  }

  /**
   * Acquires and increases the ref-count for the {@link ManagedChannel}.
   *
   * @param channelKey the channel key
   * @param conf the Alluxio configuration
   * @return a {@link GrpcConnection}
   */
  public GrpcConnection acquireConnection(GrpcChannelKey channelKey, AlluxioConfiguration conf) {
    // Get a connection key.
    GrpcConnectionKey connectionKey = getConnectionKey(channelKey, conf);
    // Acquire connection.
    CountingReference<ManagedChannel> connectionRef =
        mChannels.compute(connectionKey, (key, ref) -> {
          boolean shutdownExistingConnection = false;
          int existingRefCount = 0;
          if (ref != null) {
            // Connection exists, wait for health check.
            if (waitForConnectionReady(ref.get(), conf)) {
              LOG.debug("Acquiring an existing connection. ConnectionKey: {}. Ref-count: {}", key,
                  ref.getRefCount());

              return ref.reference();
            } else {
              // Health check failed.
              shutdownExistingConnection = true;
            }
          }
          // Existing connection should shut-down.
          if (shutdownExistingConnection) {
            // TODO(ggezer): Implement GrpcConnectionListener for receiving notification.
            existingRefCount = ref.getRefCount();
            LOG.debug("Shutting down an existing unhealthy connection. "
                + "ConnectionKey: {}. Ref-count: {}", key, existingRefCount);
            // Shutdown the channel forcefully as it's already unhealthy.
            shutdownManagedChannel(ref.get(), conf);
          }

          // Create a new managed channel.
          LOG.debug("Creating a new managed channel. ConnectionKey: {}. Ref-count:{}", key,
              existingRefCount);
          ManagedChannel managedChannel = createManagedChannel(channelKey, conf);
          // Set map reference.
          return new CountingReference(managedChannel, existingRefCount).reference();
        });

    // Wrap connection reference and the connection.
    return new GrpcConnection(connectionKey, connectionRef.get(), conf);
  }

  /**
   * Decreases ref-count of the managed channel and shuts it down if the counter reaches zero.
   * <p>
   * Decreases the ref-count of the {@link ManagedChannel} for the given address. It shuts down the
   * underlying channel if reference count reaches zero.
   *
   * @param connectionKey the connection key
   * @param conf          the Alluxio configuration
   * @throws Exception    If connection does not exist.
   */
  public void releaseConnection(GrpcConnectionKey connectionKey, AlluxioConfiguration conf) {
    mChannels.compute(connectionKey, (key, ref) -> {
      Preconditions.checkNotNull(ref, "Cannot release nonexistent connection");
      LOG.debug("Releasing connection for: {}. Ref-count: {}", key, ref.getRefCount());
      // Shutdown managed channel.
      if (ref.dereference() == 0) {
        LOG.debug("Shutting down connection after: {}", connectionKey);
        shutdownManagedChannel(ref.get(), conf);
        // Release the event-loop for the connection.
        releaseNetworkEventLoop(connectionKey.getChannelKey());
        return null;
      }
      return ref;
    });
  }

  /**
   * Creates a new gRPC connection key.
   * <p>
   * Creates a new object used to define
   * a key for this {@link GrpcConnectionPool}.
   *
   * @param   channelKey  the gRPC channel key
   * @param   conf        the Alluxio configuration
   * @return  the new {@link GrpcConnectionKey}.
   */
  private GrpcConnectionKey getConnectionKey(GrpcChannelKey channelKey, AlluxioConfiguration conf) {
    // Assign index within the network group.
    long groupIndex = mNetworkGroupCounters.get(channelKey.getNetworkGroup()).incrementAndGet();
    // Find the next slot index within the group.
    long maxConnectionsForGroup = conf.getLong(PropertyKey.Template.USER_NETWORK_MAX_CONNECTIONS
        .format(channelKey.getNetworkGroup().getPropertyCode()));
    groupIndex %= maxConnectionsForGroup;
    // Create the connection key for the chosen slot.
    return new GrpcConnectionKey(channelKey, (int) groupIndex);
  }

  /**
   * Creates and returns a managed Netty channel by given pool key.
   * <p>
   * Creates a {@link NettyChannelBuilder} with the {@link SocketAddress} from
   * {@link GrpcChannelKey#getServerAddress()}. If the address is a
   * {@link InetSocketAddress}, delays domain name system lookup in
   * order to detect changes when instantiating the builder.
   * <p>
   * Builds a new {@link ManagedChannel} and returns it.
   *
   * @param channelKey  the unique identifier for the {@link GrpcChannel}
   * @param conf        the Alluxio configuration
   * @return            the new instance of ManagedChannel created using
   *                    {@link NettyChannelBuilder#build()}
   */
  private ManagedChannel createManagedChannel(GrpcChannelKey channelKey,
      AlluxioConfiguration conf) {
    // Create netty channel builder with the address from channel key.
    NettyChannelBuilder channelBuilder;
    SocketAddress address = channelKey.getServerAddress().getSocketAddress();
    if (address instanceof InetSocketAddress) {
      InetSocketAddress inetServerAddress = (InetSocketAddress) address;
      // This constructor delays DNS lookup to detect changes
      channelBuilder = NettyChannelBuilder.forAddress(inetServerAddress.getHostName(),
          inetServerAddress.getPort());
    } else {
      channelBuilder = NettyChannelBuilder.forAddress(address);
    }
    // Apply default channel options for the multiplex group.
    channelBuilder = applyGroupDefaults(channelKey, channelBuilder, conf);
    // Build netty managed channel.
    return channelBuilder.build();
  }

  /**
   * Creates and returns a new builder for a Netty channel.
   * <p>
   * Instantiates a new object of type {@link NettyChannelBuilder} and
   * sets its properties based on the provided configurations.
   * <p>
   * Returns the created builder.
   *
   * @param key             the gRPC channel key
   * @param channelBuilder  the Netty channel builder
   * @param conf            the Alluxio configuration used to determine:
   *                            1) how long the channel builder should be kept alive for;
   *                            2) the maximum message size allowed for a single gRPC frame;
   *                            3) the flow control window in bytes;
   *                            4) the channel type, such as {@link io.netty.channel.epoll.EpollSocketChannel}
   *                            or {@link io.netty.channel.socket.nio.NioSocketChannel}
   *                            5) the {@link EventLoopGroup} to be used by the Netty transport;
   * @return  a new Netty channel builder with the provided configurations,
   *          which will be used to help simplify construction of channels
   *          using the Netty transport
   */
  private NettyChannelBuilder applyGroupDefaults(GrpcChannelKey key,
      NettyChannelBuilder channelBuilder, AlluxioConfiguration conf) {
    long keepAliveTimeMs = conf.getMs(PropertyKey.Template.USER_NETWORK_KEEPALIVE_TIME_MS
        .format(key.getNetworkGroup().getPropertyCode()));
    long keepAliveTimeoutMs = conf.getMs(PropertyKey.Template.USER_NETWORK_KEEPALIVE_TIMEOUT_MS
        .format(key.getNetworkGroup().getPropertyCode()));
    long inboundMessageSizeBytes =
        conf.getBytes(PropertyKey.Template.USER_NETWORK_MAX_INBOUND_MESSAGE_SIZE
            .format(key.getNetworkGroup().getPropertyCode()));
    long flowControlWindow = conf.getBytes(PropertyKey.Template.USER_NETWORK_FLOWCONTROL_WINDOW
        .format(key.getNetworkGroup().getPropertyCode()));
    Class<? extends Channel> channelType = NettyUtils.getChannelClass(
        !(key.getServerAddress().getSocketAddress() instanceof InetSocketAddress),
        PropertyKey.Template.USER_NETWORK_NETTY_CHANNEL
            .format(key.getNetworkGroup().getPropertyCode()),
        conf);
    EventLoopGroup eventLoopGroup = acquireNetworkEventLoop(key, conf);

    // Update the builder.
    channelBuilder.keepAliveTime(keepAliveTimeMs, TimeUnit.MILLISECONDS);
    channelBuilder.keepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS);
    channelBuilder.maxInboundMessageSize((int) inboundMessageSizeBytes);
    channelBuilder.flowControlWindow((int) flowControlWindow);
    channelBuilder.channelType(channelType);
    channelBuilder.eventLoopGroup(eventLoopGroup);
    // Use plaintext
    channelBuilder.usePlaintext();

    return channelBuilder;
  }

  /**
   * Returns {@code true} if given managed channel is ready.
   */
  private boolean waitForConnectionReady(ManagedChannel managedChannel, AlluxioConfiguration conf) {
    long healthCheckTimeoutMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);
    try {
      Boolean res = CommonUtils.waitForResult("channel to be ready", () -> {
        ConnectivityState currentState = managedChannel.getState(true);
        switch (currentState) {
          case READY:
            return true;
          case TRANSIENT_FAILURE:
          case SHUTDOWN:
            return false;
          case IDLE:
          case CONNECTING:
            return null;
          default:
            return null;
        }
      }, (b) -> b != null, WaitForOptions.defaults().setTimeoutMs((int) healthCheckTimeoutMs));
      return res;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  /**
   * Attempts to gracefully shut down the managed channel.
   * <p>
   * Tries to gracefully shut down the managed channel.
   * Falls back to forceful shutdown if graceful shutdown
   * times out.
   */
  private void shutdownManagedChannel(ManagedChannel managedChannel, AlluxioConfiguration conf) {
    // Close the gRPC managed-channel if not shut down already.
    if (!managedChannel.isShutdown()) {
      long gracefulTimeoutMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_GRACEFUL_TIMEOUT);
      managedChannel.shutdown();
      try {
        if (!managedChannel.awaitTermination(gracefulTimeoutMs, TimeUnit.MILLISECONDS)) {
          LOG.warn("Timed out gracefully shutting down connection: {}. ", managedChannel);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Allow thread to exit.
      }
    }
    // Forceful shut down if still not terminated.
    if (!managedChannel.isTerminated()) {
      long timeoutMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_TIMEOUT);

      managedChannel.shutdownNow();
      try {
        if (!managedChannel.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
          LOG.warn("Timed out forcefully shutting down connection: {}. ", managedChannel);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Allow thread to exit.
      }
    }
  }

  /**
   * Acquires event loop for the network group of a given gRPC channel key.
   * <p>
   * Checks if there is already an event loop linked to the network group
   * to which the {@code channelKey} belongs. Returns the event loop if
   * found, increasing its {@link CountingReference#mRefCount}.
   * <p>
   * Creates a new event loop if none was found for the network group,
   * saving it in {@link #mEventLoops}. Returns the created event loop.
   *
   * @param   channelKey  the gRPC channel key from which to
   *                    get the network group
   * @param   conf        the Alluxio configuration
   * @return  the {@link EventLoopGroup} for the provided {@code channelKey}
   */
  private EventLoopGroup acquireNetworkEventLoop(GrpcChannelKey channelKey,
      AlluxioConfiguration conf) {
    return mEventLoops.compute(channelKey.getNetworkGroup(), (key, v) -> {
      // Increment and return if event-loop found.
      if (v != null) {
        LOG.debug("Acquiring an existing event-loop for {}. Ref-Count:{}",
            channelKey, v.getRefCount());
        v.reference();
        return v;
      }

      // Create a new event-loop.
      ChannelType nettyChannelType =
          NettyUtils.getChannelType(PropertyKey.Template.USER_NETWORK_NETTY_CHANNEL
              .format(key.getPropertyCode()), conf);
      int nettyWorkerThreadCount =
          conf.getInt(PropertyKey.Template.USER_NETWORK_NETTY_WORKER_THREADS
              .format(key.getPropertyCode()));

      v = new CountingReference<>(
          NettyUtils.createEventLoop(nettyChannelType, nettyWorkerThreadCount, String.format(
              "alluxio-client-netty-event-loop-%s-%%d", key.name()), true),
          1);
      LOG.debug(
          "Created a new event loop. NetworkGroup: {}. NettyChannelType: {}, NettyThreadCount: {}",
          key, nettyChannelType, nettyWorkerThreadCount);

      return v;
    }).get();
  }

  /**
   * Attempts to release network event loop for a provided gRPC channel key.
   * <p>
   * Searches for the value {@link CountingReference<EventLoopGroup>} assigned to
   * the key {@link GrpcChannelKey#getNetworkGroup()} in the ConcurrentMap
   * {@link #mEventLoops}. Throws an exception if the value is null.
   * <p>
   * Checks whether the event loop group should be shutdown by comparing the return value
   * of {@link CountingReference#dereference()} and comparing it to zero. Shuts down the server
   * and sets its reference to null if true; otherwise keeps everything as is.
   *
   * @param channelKey             the gRPC channel key from which the network group
   *                               will be provided
   * @throws NullPointerException  If the provided gRPC channel key points to null.
   */
  private void releaseNetworkEventLoop(GrpcChannelKey channelKey) {
    mEventLoops.compute(channelKey.getNetworkGroup(), (key, ref) -> {
      Preconditions.checkNotNull(ref, "Cannot release nonexistent event-loop");
      LOG.debug("Releasing event-loop for: {}. Ref-count: {}", channelKey, ref.getRefCount());
      if (ref.dereference() == 0) {
        LOG.debug("Shutting down event-loop: {}", ref.get());
        // Shutdown the event-loop gracefully.
        ref.get().shutdownGracefully();
        // No need to wait for event-loop shutdown.
        return null;
      }
      return ref;
    });
  }

  /**
   * Used as reference counting wrapper over instance of type {@link T}.
   */
  private class CountingReference<T> {
    private T mObject;
    private AtomicInteger mRefCount;

    private CountingReference(T object, int initialRefCount) {
      mObject = object;
      mRefCount = new AtomicInteger(initialRefCount);
    }

    /**
     * Returns the underlying object, increasing the reference count.
     * <p>
     * Increments the {@link #mRefCount} of the underlying {@link #mObject}
     * of type {@code T} and returns this {@link CountingReference<T>}.
     *
     * @return  the underlying object after increasing ref-count
     */
    private CountingReference reference() {
      mRefCount.incrementAndGet();
      return this;
    }

    /**
     * Decrements the reference count for the underlying object.
     * <p>
     * Decrements the {@link #mRefCount} for the underlying {@link #mObject}
     * and returns the updated value for the reference count.
     *
     * @return  the current reference count after dereference
     */
    private int dereference() {
      return mRefCount.decrementAndGet();
    }

    /**
     * Returns current reference count of the underlying object.
     * <p>
     * Gets the current {@link #mRefCount} for the underlying {@link #mObject}.
     *
     * @return the current reference count
     */
    private int getRefCount() {
      return mRefCount.get();
    }

    /**
     * Returns the underlying object without changing the reference count.
     *
     * @return the {@link #mObject} without changing the {@link #mRefCount}.
     */
    private T get() {
      return mObject;
    }
  }
}
