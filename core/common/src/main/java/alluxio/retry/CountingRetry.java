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

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An option which allows retrying based on maximum count.
 */
@NotThreadSafe
public class CountingRetry implements RetryPolicy {

  private final int mMaxRetries;
  private int mAttemptCount = 0;

  /**
   * Constructs a retry facility which allows max number of retries.
   *
   * @param maxRetries max number of retries
   */
  public CountingRetry(int maxRetries) {
    Preconditions.checkArgument(maxRetries >= 0, "Max retries must be a non-negative number");
    mMaxRetries = maxRetries;
  }

  @Override
  public int getAttemptCount() {
    return mAttemptCount;
  }

  /**
   * Checks if a new attempt can be made given the maximum number for retries in this policy.
   * <p>
   * Returns a boolean value indicating whether a new attempt can be made based
   * on how many attempts were registered by the {@link #mAttemptCount} and the the number of
   * {@link #mMaxRetries} established by this {@link CountingRetry}.
   *
   * @return  a boolean value representing whether a new retry
   *          can be made based on this {@link RetryPolicy}.
   */
  @Override
  public boolean attempt() {
    if (mAttemptCount <= mMaxRetries) {
      mAttemptCount++;
      return true;
    }
    return false;
  }

  /**
   * Reset the count of retries.
   */
  public void reset() {
    mAttemptCount = 0;
  }
}
