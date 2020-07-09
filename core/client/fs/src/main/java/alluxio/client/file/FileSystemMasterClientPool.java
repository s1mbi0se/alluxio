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

package alluxio.client.file;

import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.resource.DynamicResourcePool;
import alluxio.util.ThreadFactoryUtils;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A fixed pool of FileSystemMasterClient instances.
 */
@ThreadSafe
public final class FileSystemMasterClientPool extends DynamicResourcePool<FileSystemMasterClient> {
  private final MasterClientContext mMasterContext;
  private final long mGcThresholdMs;

  private static final int FS_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE = 1;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(FS_MASTER_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("FileSystemMasterClientPoolGcThreads-%d", true));

  /**
   * Creates a new file system master client pool.
   *
   * @param ctx information for connecting to processes in the cluster
   */
  public FileSystemMasterClientPool(MasterClientContext ctx) {
    super(Options.defaultOptions()
        .setMinCapacity(ctx.getClusterConf()
            .getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MIN))
        .setMaxCapacity(ctx.getClusterConf()
            .getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX))
        .setGcIntervalMs(
            ctx.getClusterConf().getMs(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_GC_INTERVAL_MS))
        .setGcExecutor(GC_EXECUTOR));
    mMasterContext = ctx;
    mGcThresholdMs =
        ctx.getClusterConf().getMs(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_GC_THRESHOLD_MS);
  }

  /**
   * Closes the provided file system master client. After this, the resource should not be used.
   *
   * @param   client the object of type {@link FileSystemMasterClient}
   *                 to be closed
   * @throws  RuntimeException  if an {@link IOException} is thrown while
   *                            trying to close the provided {@code client}.
   */
  @Override
  protected void closeResource(FileSystemMasterClient client) {
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a new client to use for interacting with a file system master.
   * <p>
   * Creates and returns a new {@link FileSystemMasterClient} through
   * {@link FileSystemMasterClient.Factory#create} and using the existing
   * {@link FileSystemMasterClientPool#mMasterContext}.
   *
   * @return  a new resource for interacting with a file system master
   */
  @Override
  protected FileSystemMasterClient createNewResource() {
    return FileSystemMasterClient.Factory.create(mMasterContext);
  }

  @Override
  protected boolean isHealthy(FileSystemMasterClient client) {
    return client.isConnected();
  }

  @Override
  protected boolean shouldGc(ResourceInternal<FileSystemMasterClient> clientResourceInternal) {
    return System.currentTimeMillis() - clientResourceInternal
        .getLastAccessTimeMs() > mGcThresholdMs;
  }
}
