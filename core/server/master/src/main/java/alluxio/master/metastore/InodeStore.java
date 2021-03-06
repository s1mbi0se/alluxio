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

package alluxio.master.metastore;

import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.journal.checkpoint.Checkpointed;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Inode metadata storage.
 *
 * The inode store manages metadata about individual inodes, as well as the parent-child
 * relationships between them.
 *
 * Writes to the inode store happen atomically, so it is safe to concurrently read and write the
 * same key. It is undefined whether the read will see the old value or the new value.
 *
 * The inode store needs a way to temporarily prevent modification to individual inodes or edges. It
 * does this by acquiring inode lock manager write locks. This requires that the user acquires inode
 * manager locks when making any modifications to the inode store.
 */
@ThreadSafe
public interface InodeStore extends ReadOnlyInodeStore, Checkpointed, Closeable {
  /**
   * Gets a mutable representation of the specified inode.
   *
   * Mutating the returned inode requires holding an inode lock manager read or write lock on the
   * inode.
   *
   * @param id an inode id
   * @param option read options
   * @return the inode with the given id, if it exists
   */
  Optional<MutableInode<?>> getMutable(long id, ReadOption option);

  /**
   * Gets the mutable inode with the provided ID if it exists using the default read option.
   * <p>
   * Returns the {@link MutableInode} with the provided {@code id} if it exists, otherwise
   * returns an empty {@link Optional}. The {@link ReadOption} for this operation is set to
   * {@code DEFAULT}.
   *
   * @param id the {@link InodeView} ID
   * @return the mutable inode for the provided {@code id}, if it exists. Returns empty
   *         otherwise.
   */
  default Optional<MutableInode<?>> getMutable(long id) {
    return getMutable(id, ReadOption.defaults());
  }

  @Override
  default Optional<Inode> get(long id, ReadOption option) {
    return getMutable(id, option).map(Inode::wrap);
  }

  /**
   * Removes an inode from the inode store. This does *not* remove the edge leading to the inode.
   *
   * This method requires an inode lock manager read or write lock on the removed inode.
   *
   * @param inodeId an inode to remove
   */
  void remove(Long inodeId);

  /**
   * Removes an inode from the inode store. This does *not* remove the edge leading to the inode.
   *
   * This method requires an inode lock manager read or write lock on the removed inode.
   *
   * @param inode an inode to remove
   */
  default void remove(InodeView inode) {
    remove(inode.getId());
  }

  /**
   * Removes an inode and the edge leading to it from the inode store.
   *
   * This method requires an inode lock manager read or write locks on the removed inode and edge.
   *
   * @param inode an inode to remove
   */
  default void removeInodeAndParentEdge(InodeView inode) {
    remove(inode);
    removeChild(inode.getParentId(), inode.getName());
  }

  /**
   * Adds the given inode, or overwrites it if it already exists.
   * <p>
   * If it is known that the inode is new, prefer {@link #writeNewInode(MutableInode)}.
   * <p>
   * This method requires an {@link InodeLockManager} read or write lock on the written inode.
   *
   * @param inode the {@link MutableInode} to write to this {@link InodeStore}
   */
  void writeInode(MutableInode<?> inode);

  /**
   * Adds a new inode.
   *
   * This method is similar to {@link #writeInode(MutableInode)}, but with an added information that
   * the inode is new. This allows some inode stores to perform extra optimizations.
   *
   * This method requires an inode lock manager read or write lock on the written inode.
   *
   * @param inode the inode to write
   */
  default void writeNewInode(MutableInode<?> inode) {
    writeInode(inode);
  }

  /**
   * Creates a write batch. Not all implementations support this, so callers should first check
   * {@link #supportsBatchWrite()}.
   *
   * @return a {@link WriteBatch} which can be used to perform a batched write
   */
  default WriteBatch createWriteBatch() {
    throw new UnsupportedOperationException(
        "batch writes are not supported for " + getClass().getSimpleName());
  }

  /**
   * @return whether the inode store implementation supports batch writes
   */
  default boolean supportsBatchWrite() {
    return false;
  }

  /**
   * Removes all inodes and edges.
   */
  void clear();

  /**
   * Makes an inode the child of another inode.
   * <p>
   * Implementations should make the inode with the provided
   * {@code childId}, {@code childName} and the child of the inode
   * with the provided {@code parentId}.
   * <p>
   * This method requires an {@link InodeLockManager} read or write locks
   * on the added {@link alluxio.master.file.meta.Edge}.
   *
   * @param parentId the parent inode ID
   * @param childName the child inode name
   * @param childId the child inode ID
   */
  void addChild(long parentId, String childName, Long childId);

  /**
   * Makes an inode the child of the specified parent.
   *
   * This method requires an inode lock manager read or write locks on the added edge.
   *
   * @param parentId the parent id
   * @param child the child inode
   */
  default void addChild(long parentId, InodeView child) {
    addChild(parentId, child.getName(), child.getId());
  }

  /**
   * Removes a child from a parent inode.
   *
   * This method requires an inode lock manager read or write locks on the removed edge.
   *
   * @param parentId the parent inode id
   * @param name the child name
   */
  void removeChild(long parentId, String name);

  @Override
  default void close() {}

  /**
   * Used to perform batched writes. Call {@link #createWriteBatch()} to use batched writes.
   *
   * Write batches may or may not be applied atomically.
   */
  interface WriteBatch extends AutoCloseable {
    /**
     * Adds an inode to the write batch. This method serializes the inode, so future modifications
     * to the inode will not affect the write batch.
     *
     * @param inode the inode to add
     */
    void writeInode(MutableInode<?> inode);

    /**
     * Adds an inode removal operation to the write batch.
     *
     * @param key the key to remove
     */
    void removeInode(Long key);

    /**
     * Adds an edge to the write batch.
     *
     * @param parentId the parent id
     * @param childName the child name
     * @param childId the child id
     */
    void addChild(Long parentId, String childName, Long childId);

    /**
     * Adds an edge removal operation to the write batch.
     *
     * @param parentId the parent id
     * @param childName the child id
     */
    void removeChild(Long parentId, String childName);

    /**
     * Performs the batched write.
     */
    void commit();

    @Override
    void close();
  }

  /**
   * Factory for creating inode stores.
   */
  interface Factory extends Function<InodeLockManager, InodeStore> {}
}
