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
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.login.AppLoginModule;
import alluxio.security.login.LoginModuleConfiguration;
import alluxio.util.SecurityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * UserState implementation for the simple authentication schemes.
 */
public class SimpleUserState extends BaseUserState {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleUserState.class);

  /**
   * Factory class to create the user state.
   */
  public static class Factory implements UserStateFactory {
    /**
     * Returns a new user state implementation for simple authentication schemes.
     * <p>
     * Creates a new object of type {@link AuthType} based on the provided
     * {@link AlluxioConfiguration} and checks whether the authentication
     * type is {@link AuthType#SIMPLE} or {@link AuthType#CUSTOM}, in which
     * case a new object of type {@link SimpleUserState} is created with the
     * provided {@link Subject} and configuration, and returned.
     * <p>
     * Returns null if the authentication type is unsupported.
     *
     * @param subject   the subject
     * @param conf      the Alluxio configuration
     * @param isServer  a boolean value indicating whether this is from a server process
     * @return          the new simple user state if the authentication type is supported; otherwise, null
     */
    @Override
    public UserState create(Subject subject, AlluxioConfiguration conf, boolean isServer) {
      AuthType authType = conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      if (authType == AuthType.SIMPLE || authType == AuthType.CUSTOM) {
        return new SimpleUserState(subject, conf);
      }
      LOG.debug("N/A: auth type is not SIMPLE or CUSTOM. authType: {}", authType.getAuthName());
      return null;
    }
  }

  private SimpleUserState(Subject subject, AlluxioConfiguration conf) {
    super(subject, conf);
  }

  /**
   * Logs in an Alluxio user.
   * <p>
   * Gets the {@link PropertyKey#SECURITY_LOGIN_USERNAME} from {@link #mConf} if
   * key exists. Sets username to an empty string otherwise.
   * <p>
   * The username is used to create a new {@code LoginContext} with a {@link AuthType#SIMPLE}
   * authentication type, the {@link #mSubject} of this instance, the {@link User} {@code ClassLoader},
   * a new {@link LoginModuleConfiguration} instance, and a new object of type
   * {@link AppLoginModule.AppCallbackHandler(String)} with the username String.
   * <p>
   * Attempts to login using the created {@code LoginContext}. Throws an exception if the login cannot
   * be made.
   * <p>
   * Checks the number of Alluxio {@link User}s present in {@code mSubject}. Throws an
   * exception if none or more than one are found.
   * <p>
   * Returns the {@code User} that was found.
   *
   * @return  the Alluxio {@code User} that was successfully logged in
   * @throws  UnauthenticatedException  if a {@link LoginException} is thrown, no Alluxio {@code User}
   *                                    is found in {@code mSubject} or multiple Alluxio {@code User}s
   *                                    are found in {@code mSubject}.
   */
  @Override
  public User login() throws UnauthenticatedException {
    String username = "";
    if (mConf.isSet(PropertyKey.SECURITY_LOGIN_USERNAME)) {
      username = mConf.get(PropertyKey.SECURITY_LOGIN_USERNAME);
    }
    try {
      // Use the class loader of User.class to construct the LoginContext. LoginContext uses this
      // class loader to dynamically instantiate login modules. This enables
      // Subject#getPrincipals to use reflection to search for User.class instances.
      LoginContext loginContext =
          SecurityUtils.createLoginContext(AuthType.SIMPLE, mSubject, User.class.getClassLoader(),
              new LoginModuleConfiguration(), new AppLoginModule.AppCallbackHandler(username));
      loginContext.login();
    } catch (LoginException e) {
      throw new UnauthenticatedException("Failed to login: " + e.getMessage(), e);
    }

    LOG.debug("login subject: {}", mSubject);
    Set<User> userSet = mSubject.getPrincipals(User.class);
    if (userSet.isEmpty()) {
      throw new UnauthenticatedException("Failed to login: No Alluxio User is found.");
    }
    if (userSet.size() > 1) {
      StringBuilder msg = new StringBuilder(
          "Failed to login: More than one Alluxio Users are found:");
      for (User user : userSet) {
        msg.append(" ").append(user.toString());
      }
      throw new UnauthenticatedException(msg.toString());
    }
    return userSet.iterator().next();
  }
}
