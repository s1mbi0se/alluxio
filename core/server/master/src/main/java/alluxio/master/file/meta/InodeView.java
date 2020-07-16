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

package alluxio.master.file.meta;

import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.proto.meta.InodeMeta;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclActions;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.wire.FileInfo;
import alluxio.grpc.TtlAction;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Read-only view of an inode.
 */
public interface InodeView extends JournalEntryRepresentable, Comparable<InodeView> {

  /**
   * @return the create time, in milliseconds
   */
  long getCreationTimeMs();

  /**
   * @return the group of the inode
   */
  String getGroup();

  /**
   * Gets and returns the ID of the inode.
   * <p>
   * Implementations should return a long value corresponding
   * to the ID of the {@link Inode}.
   *
   * @return  the id corresponding to the {@link Inode}
   */
  long getId();

  /**
   * @return the ttl of the file
   */
  long getTtl();

  /**
   * @return the {@link TtlAction}
   */
  TtlAction getTtlAction();

  /**
   * @return the last modification time, in milliseconds
   */
  long getLastModificationTimeMs();

  /**
   * @return the last access time, in milliseconds
   */
  long getLastAccessTimeMs();

  /**
   * Gets and returns the name of the inode as a {@code String}.
   * <p>
   * Implementations should fetch and return a {@code String} corresponding
   * to the name of the inode.
   *
   * @return  the name of the inode
   */
  String getName();

  /**
   * @return the mode of the inode
   */
  short getMode();

  /**
   * @return the {@link PersistenceState} of the inode
   */
  PersistenceState getPersistenceState();

  /**
   * @return the id of the parent folder
   */
  long getParentId();

  /**
   * @return the owner of the inode
   */
  String getOwner();

  /**
   * @return any extended attributes on the inode
   */
  @Nullable
  Map<String, byte[]> getXAttr();

  /**
   * @return true if the inode is deleted, false otherwise
   */
  boolean isDeleted();

  /**
   * Checks whether the inode is a directory returning a boolean value indicating if it's or not.
   *
   * @return  a boolean value representing whether the inode
   *          is a directory. Returns {@code true} if it is
   *          a directory, {@code false} otherwise.
   */
  boolean isDirectory();

  /**
   * Checks whether the inode is a file returning a boolean value indicating if it is one or not.
   *
   * @return  a boolean representing whether the {@link Inode}
   *          implementing this {@link InodeView} is a file.
   *          Returns {@code true} if it is a file, {@code false}
   *          otherwise.
   */
  boolean isFile();

  /**
   * @return true if the inode is pinned, false otherwise
   */
  boolean isPinned();

  /**
   * @return true if the file has persisted, false otherwise
   */
  boolean isPersisted();

  /**
   * @return the pinned medium types set
   */
  Set<String> getMediumTypes();

  /**
   * @return the UFS fingerprint
   */
  String getUfsFingerprint();

  /**
   * @return the access control list
   */
  AccessControlList getACL();

  /**
   * @return the default ACL associated with this inode
   * @throws UnsupportedOperationException if the inode is a file
   */
  DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException;

  /**
   * Generates a {@link FileInfo} of the file or folder.
   *
   * @param path the path of the file
   * @return generated {@link FileInfo}
   */
  FileInfo generateClientFileInfo(String path);

  /**
   * Checks whether the user or one of the groups has the permission to take the action.
   *
   *
   * @param user the user checking permission
   * @param groups the groups the user belongs to
   * @param action the action to take
   * @return whether permitted to take the action
   * @see AccessControlList#checkPermission(String, List, AclAction)
   */
  boolean checkPermission(String user, List<String> groups, AclAction action);

  /**
   * Gets the permitted actions for a user.
   *
   * @param user the user
   * @param groups the groups the user belongs to
   * @return the permitted actions
   * @see AccessControlList#getPermission(String, List)
   */
  AclActions getPermission(String user, List<String> groups);

  /**
   * @return the protocol buffer representation of the inode
   */
  InodeMeta.Inode toProto();

  @Override
  default int compareTo(InodeView o) {
    return getName().compareTo(o.getName());
  }
}
