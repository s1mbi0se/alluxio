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

import alluxio.time.TimeContext;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A retry policy which uses exponential backoff and a maximum duration time bound.
 *
 * A final retry will be performed at the time bound before giving up.
 *
 * For example, with initial sleep 10ms, maximum sleep 100ms, and maximum duration 500ms, the sleep
 * timings would be [10, 20, 40, 80, 100, 100, 100, 50], assuming the operation being retries takes
 * no time. The 50 at the end is because the previous times add up to 450, so the mechanism sleeps
 * for only 50ms before the final attempt.
 *
 * However, those are just the base sleep timings. For each sleep time, we multiply by a random
 * number from 1 to 1.1 to add jitter to avoid hotspotting.
 */
public final class ExponentialTimeBoundedRetry extends TimeBoundedRetry {
  private final Duration mMaxSleep;
  private Duration mNextSleep;
  boolean mSkipInitialSleep;
  boolean mSleepSkipped;

  /**
   * See {@link Builder}.
   */
  private ExponentialTimeBoundedRetry(TimeContext timeCtx, Duration maxDuration,
      Duration initialSleep, Duration maxSleep, boolean skipInitialSleep) {
    super(timeCtx, maxDuration);
    mMaxSleep = maxSleep;
    mNextSleep = initialSleep;
    mSkipInitialSleep = skipInitialSleep;
  }

  @Override
  protected Duration computeNextWaitTime() {
    if (mSkipInitialSleep && !mSleepSkipped) {
      mSleepSkipped = true;
      return Duration.ofNanos(0);
    }
    Duration next = mNextSleep;
    mNextSleep = mNextSleep.multipliedBy(2);
    if (mNextSleep.compareTo(mMaxSleep) > 0) {
      mNextSleep = mMaxSleep;
    }
    // Add jitter.
    long jitter = Math.round(ThreadLocalRandom.current().nextDouble(0.1) * next.toMillis());
    return next.plusMillis(jitter);
  }

  /**
   * Creates an object of type Builder used to build
   * a new instance of ExponentialTimeBoundedRetry.
   * <p>
   * This makes sure an object of type
   * ExponentialTimeBoundedRetry can be safely
   * instantiated and avoid future problems.
   *
   * @return  an object of type Builder used to build an
   *          object of type ExponentialTimeBoundedRetry
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for time bounded exponential retry mechanisms.
   * <p>
   * It is used especially for setting time limits for
   * I/O-bound operations.
   */
  public static class Builder {
    private TimeContext mTimeCtx = TimeContext.SYSTEM;
    private Duration mMaxDuration;
    private Duration mInitialSleep;
    private Duration mMaxSleep;
    private boolean mSkipInitialSleep = false;

    /**
     * @param timeCtx time context
     * @return the builder
     */
    public Builder withTimeCtx(TimeContext timeCtx) {
      mTimeCtx = timeCtx;
      return this;
    }

    /**
     * @param maxDuration max total duration to retry for
     * @return the builder
     */
    public Builder withMaxDuration(Duration maxDuration) {
      mMaxDuration = maxDuration;
      return this;
    }

    /**
     * Defines the initial sleep time for this
     * new instance of ExponentialTimeBoundedRetry.
     * @param initialSleep initial sleep interval between retries
     * @return the builder
     */
    public Builder withInitialSleep(Duration initialSleep) {
      mInitialSleep = initialSleep;
      return this;
    }

    /**
     * Defines the maximum sleep time for this
     * new instance of ExponentialTimeBoundedRetry.
     *
     * @param maxSleep maximum sleep interval between retries
     * @return the builder
     */
    public Builder withMaxSleep(Duration maxSleep) {
      mMaxSleep = maxSleep;
      return this;
    }

    /**
     * Skips first sleep for this new
     * instance of ExponentialTimeBoundedRetry.
     *
     * @return the builder
     */
    public Builder withSkipInitialSleep() {
      mSkipInitialSleep = true;
      return this;
    }

    /**
     * Instantiates a new ExponentialTimeBoundedRetry,
     * used to set the rules for operations that are
     * I/O-bound.
     * <p>
     * The arguments passed on to the constructor are
     * defined by {@link alluxio.retry.ExponentialTimeBoundedRetry.Builder}.
     *
     * @return  the built retry mechanism
     */
    public ExponentialTimeBoundedRetry build() {
      return new ExponentialTimeBoundedRetry(
          mTimeCtx, mMaxDuration, mInitialSleep, mMaxSleep, mSkipInitialSleep);
    }
  }
}
