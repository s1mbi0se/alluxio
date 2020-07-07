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

package alluxio.retry;

import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A retry policy which allows retrying until a specified timeout is reached.
 */
@NotThreadSafe
public class TimeoutRetry implements RetryPolicy {

  private final long mRetryTimeoutMs;
  private final long mSleepMs;
  private long mStartMs = 0;
  private int mAttemptCount = 0;

  /**
   * Constructs a retry facility which allows retrying until a specified timeout is reached.
   *
   * @param retryTimeoutMs maximum period of time to retry for, in milliseconds
   * @param sleepMs time in milliseconds to sleep before retrying
   */
  public TimeoutRetry(long retryTimeoutMs, int sleepMs) {
    Preconditions.checkArgument(retryTimeoutMs > 0, "Retry timeout must be a positive number");
    Preconditions.checkArgument(sleepMs >= 0, "sleepMs cannot be negative");
    mRetryTimeoutMs = retryTimeoutMs;
    mSleepMs = sleepMs;
  }

  @Override
  public int getAttemptCount() {
    return mAttemptCount;
  }

  /**
   * Checks if a new attempt to execute a given method can be made before the established timeout.
   * <p>
   * Sets {@link #mStartMs} to the current time if this is the first attempt
   * and returns true right away.
   * <p>
   * Sleeps for the established time if {@link #mSleepMs} is a positive long.
   * <p>
   * Returns true if the time elapsed between now and the first attempt is
   * smaller than or equal to the established {@link #mRetryTimeoutMs}.
   * Returns false otherwise.
   *
   * @return  a boolean representing whether a new attempt can
   *          be made according to this {@link RetryPolicy}
   */
  @Override
  public boolean attempt() {
    if (mAttemptCount == 0) {
      // first attempt, set the start time
      mStartMs = CommonUtils.getCurrentMs();
      mAttemptCount++;
      return true;
    }
    if (mSleepMs > 0) {
      CommonUtils.sleepMs(mSleepMs);
    }
    if ((CommonUtils.getCurrentMs() - mStartMs) <= mRetryTimeoutMs) {
      mAttemptCount++;
      return true;
    }
    return false;
  }
}
