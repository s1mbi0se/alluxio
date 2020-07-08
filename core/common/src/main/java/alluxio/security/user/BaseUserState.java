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

package alluxio.security.user;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.security.auth.Subject;

/**
 * Base implementation of {@link UserState}.
 */
public abstract class BaseUserState implements UserState {
  private static final Logger LOG = LoggerFactory.getLogger(BaseUserState.class);

  protected final Subject mSubject;
  protected volatile User mUser;
  // TODO(gpang): consider removing conf, and simply passing in implementation-specific values.
  protected AlluxioConfiguration mConf;

  /**
   * @param subject the subject
   * @param conf the Alluxio configuration
   */
  public BaseUserState(Subject subject, AlluxioConfiguration conf) {
    mSubject = subject;
    mConf = conf;
    mUser = getUserFromSubject();
  }

  /**
   *  Returns the subject representing the user.
   *  <p>
   *  Attempts to login with the user's credentials
   *  and then returns the subject of the user.
   *
   * @return  {@link BaseUserState#mSubject}, an object of type
   *          {@link Subject} representing the user to be logged in.
   */
  @Override
  public Subject getSubject() {
    try {
      tryLogin();
    } catch (UnauthenticatedException e) {
      if (!LOG.isDebugEnabled()) {
        LOG.warn("Subject login failed from {}: {}", this.getClass().getName(), e.getMessage());
      } else {
        LOG.error(String.format("Subject login failed from %s: ", this.getClass().getName()), e);
      }
    }
    return mSubject;
  }

  /**
   * Attempts to get the user for this instance.
   * <p>
   * Attempts to login through {@link #tryLogin}
   * and return the {@link User} for this instance.
   *
   * @return  the existing {@link #mUser}
   * @throws  UnauthenticatedException if the login fails
   */
  @Override
  public User getUser() throws UnauthenticatedException {
    tryLogin();
    return mUser;
  }

  /**
   * Logs in the Alluxio user.
   * <p>
   * Attempts to log in the {@link #mUser}.
   *
   * @return  the Alluxio {@link User} that was logged in
   * @throws UnauthenticatedException if an exception occurs
   *                                  while trying to log in
   */
  protected abstract User login() throws UnauthenticatedException;

  /**
   * Attempts to login the user if it is not already logged in.
   * <p>
   * Checks whether the user is not yet logged in by checking if
   * {@link #mUser} is null. Synchronizes this implementation
   * of {@link UserState} to make sure concurrent threads
   * cannot manipulate this object while a second check is
   * made to ensure whether user has not been logged in yet.
   * <p>
   * Logs in the user if not logged in yet.
   *
   * @throws UnauthenticatedException if an exception occurs
   *                                  while trying to {@link #login}
   */
  private void tryLogin() throws UnauthenticatedException {
    if (mUser == null) {
      synchronized (this) {
        if (mUser == null) {
          mUser = login();
        }
      }
    }
  }

  /**
   * @return the {@link User} found in the subject, or null if the subject does not contain one
   */
  private User getUserFromSubject() {
    Set<User> userSet = mSubject.getPrincipals(User.class);
    if (userSet.isEmpty()) {
      return null;
    }
    return userSet.iterator().next();
  }

  @Override
  public User relogin() throws UnauthenticatedException {
      // Specific implementations can change the behavior.
    tryLogin();
    return mUser;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSubject);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null) {
      return false;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }
    BaseUserState that = (BaseUserState) o;
    return Objects.equal(mSubject, that.mSubject);
  }
}
