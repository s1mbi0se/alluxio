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

package alluxio.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for the {@link ThreadFactory} class.
 */
@ThreadSafe
public final class ThreadFactoryUtils {
  private ThreadFactoryUtils() {}

  /**
   * Creates a thread factory that spawns off threads.
   * <p>
   * Returns a new object of type {@link ThreadFactory} using {@link ThreadFactoryBuilder#build()}.
   *
   * @param nameFormat  the name pattern for each thread. should contain '%d' to distinguish between
   *                    threads.
   * @param isDaemon    boolean representing whether the thread factory should create daemon threads.
   * @return            a new thread factory with the provided information
   */
  public static ThreadFactory build(final String nameFormat, boolean isDaemon) {
    return new ThreadFactoryBuilder().setDaemon(isDaemon).setNameFormat(nameFormat).build();
  }
}
