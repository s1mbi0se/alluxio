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

package alluxio.underfs.b2;

import static com.backblaze.b2.client.contentSources.B2Headers.USER_AGENT;
import static java.util.Objects.requireNonNull;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.backblaze.b2.client.B2ListFilesIterable;
import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.B2StorageClientFactory;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2AccountAuthorization;
import com.backblaze.b2.client.structures.B2Allowed;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListFileNamesRequest;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * B2 {@link UnderFileSystem} implementation based on the b2-sdk-core library.
 */
@ThreadSafe public class B2UnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(B2UnderFileSystem.class);

  public static final String B2_SCHEME = "b2://";
  private static final String FOLDER_SUFFIX = "/";
  private static final String FOLDER_FILE_INDICATOR = ".bzEmpty";

  private final B2StorageClient mB2StorageClient;
  private final String mBucketName;
  private final String mAccountId;
  private final String mBucketId;
  private final short mBucketMode;

  /**
   * Constructs a new instance of {@link B2UnderFileSystem}.
   *
   * @param alluxioURI the {@link AlluxioURI} for this UFS
   * @param conf       the configuration for this UFS
   * @return the created {@link B2UnderFileSystem} instance
   * @throws ServiceException
   */
  public static B2UnderFileSystem createInstance(AlluxioURI alluxioURI,
      UnderFileSystemConfiguration conf) throws ServiceException {
    LOG.debug("initializing B2 Storage Client");
    LOG.debug(alluxioURI.toString());
    B2StorageClient client = B2StorageClientFactory.createDefaultFactory()
        .create(conf.get(PropertyKey.B2_ACCESS_KEY), conf.get(PropertyKey.B2_SECRET_KEY),
            USER_AGENT);

    try {
      B2AccountAuthorization accountAuthorization = client.getAccountAuthorization();
      LOG.debug(accountAuthorization.toString());

      B2Allowed allowed = accountAuthorization.getAllowed();
      LOG.debug(allowed.toString());

      String accountId = accountAuthorization.getAccountId();
      List<String> capabilities = allowed.getCapabilities();

      String bucketNameFromURI = UnderFileSystemUtils.getBucketName(alluxioURI);
      LOG.debug("bucketname from uri: {}", bucketNameFromURI);

      String bucketId;
      String bucketName;
      if (allowed.getBucketId() != null) {
        bucketId = allowed.getBucketId();
        bucketName = allowed.getBucketName();
      } else {
        B2Bucket bucket = client.getBucketOrNullByName(bucketNameFromURI);
        LOG.debug(bucket.toString());
        bucketId = bucket.getBucketId();
        bucketName = bucket.getBucketName();
      }

      short bucketMode = B2Utils.translateBucketAcl(capabilities);

      return new B2UnderFileSystem(alluxioURI, conf, client, bucketId, bucketName, bucketMode,
          accountId);
    } catch (B2Exception | NullPointerException e) {
      LOG.error("Failed to instantiate B2UnderFileSystem client with key:{} and URI: {}",
          conf.get(PropertyKey.B2_ACCESS_KEY), alluxioURI);
      Throwables.propagateIfPossible(e, ServiceException.class);
    }
    return null;
  }

  protected B2UnderFileSystem(AlluxioURI alluxioURI, UnderFileSystemConfiguration conf,
      B2StorageClient b2StorageClient, String bucketId, String bucketName, short bucketMode,
      String accountId) {
    super(alluxioURI, conf);
    requireNonNull(alluxioURI, "alluxioURI is null");
    requireNonNull(conf, "conf is null");
    requireNonNull(b2StorageClient, "b2StorageClient is null");
    requireNonNull(bucketId, "bucketId is null");
    requireNonNull(bucketName, "bucketName is null");
    requireNonNull(bucketMode, "bucketMode is null");
    requireNonNull(accountId, "accountId is null");

    LOG.debug("alluxioURI: {}", alluxioURI);
    LOG.debug("buckedId: {}", bucketId);
    LOG.debug("bucketName: {}", bucketName);
    LOG.debug("bucketMode: {}", bucketMode);
    LOG.debug("accountId: {}", accountId);

    mB2StorageClient = b2StorageClient;
    mBucketId = bucketId;
    mBucketName = bucketName;
    mBucketMode = bucketMode;
    mAccountId = accountId;
  }

  @Override public boolean createEmptyObject(String key) {
    return false;
  }

  @Override protected OutputStream createObject(String key) throws IOException {
    return null;
  }

  @Override protected boolean copyObject(String src, String dst) throws IOException {
    return false;
  }

  //
  @Override protected boolean deleteObject(String key) throws IOException {
    return false;
  }

  @Override protected ObjectPermissions getPermissions() {
    LOG.debug("getPermissions");
    return new ObjectPermissions(mAccountId, mAccountId, mBucketMode);
  }

  @Nullable @Override protected ObjectStatus getObjectStatus(String key) throws IOException {
    LOG.debug("getObjectStatus");
    LOG.debug("bucketName: {}, key: ", mBucketName, key);
    try {
      B2FileVersion fileInfo = mB2StorageClient.getFileInfoByName(mBucketName, key);
      return new ObjectStatus(fileInfo.getFileName(), fileInfo.getContentSha1(),
          fileInfo.getContentLength(), fileInfo.getUploadTimestamp());
    } catch (B2Exception e) {
      if (e.getCode().equals("not_found")) {
        LOG.debug("key not found {}", key);
        return null;
      }
      throw new RuntimeException(e);
    }
  }

  @Override protected String getFolderSuffix() {
    LOG.debug("getFolderSuffix {}", FOLDER_SUFFIX);
    return FOLDER_SUFFIX;
  }

  @Override protected String getRootKey() {
    LOG.debug("getRootKey {}", B2_SCHEME + mBucketName);
    return B2_SCHEME + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options, RetryPolicy retryPolicy)
      throws IOException {
    LOG.debug("openObject");
    LOG.debug("key [{}]", key);
    LOG.debug("options [{}]", options);
    try {
      return new B2InputStream(mBucketName, key, mB2StorageClient, options.getOffset(),
          options.getLength());
    } catch (ServiceException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override public String getUnderFSType() {
    LOG.debug("getUnderFSType");
    return "b2";
  }

  @Nullable @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    LOG.debug("getObjectListingChunk({}, {})", key, recursive);
    String delimiter = recursive ? null : PATH_SEPARATOR;
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix.
    key = key.equals(PATH_SEPARATOR) ? "" : key;

    LOG.debug("key: {}", key);
    LOG.debug("delimiter: {}", delimiter);

    final B2ListFileNamesRequest.Builder builder =
        B2ListFileNamesRequest.builder(mBucketId).setMaxFileCount(getListingChunkLength(mUfsConf))
            .setPrefix(key);

    if (delimiter != null) {
      builder.setDelimiter(delimiter);
    }

    final B2ListFileNamesRequest request = builder.build();

    try {
      return new B2ObjectListingChunk(mB2StorageClient.fileNames(request));
    } catch (B2Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      return null;
    }
  }

  private final class B2ObjectListingChunk implements ObjectListingChunk {

    private final ImmutableList<B2FileVersion> mFiles;

    B2ObjectListingChunk(B2ListFilesIterable iterable) throws IOException {
      if (iterable == null) {
        throw new IOException("B2 listing result is null");
      }
      ImmutableList.Builder<B2FileVersion> builder = ImmutableList.builder();
      iterable.iterator().forEachRemaining(file -> {
        LOG.debug("object: {}", file.getFileName());
        if (!file.getFileName().endsWith(FOLDER_FILE_INDICATOR)) {
          LOG.debug(file.toString());
          builder.add(file);
        } else {
          LOG.debug("skipped {}", file.toString());
        }
      });
      mFiles = builder.build();
    }

    @Override public ObjectStatus[] getObjectStatuses() {
      return mFiles.stream().map(
          obj -> new ObjectStatus(obj.getFileName(), obj.getContentSha1(), obj.getContentLength(),
              obj.getUploadTimestamp())).collect(Collectors.toList())
          .toArray(new ObjectStatus[] {});
    }

    @Override public String[] getCommonPrefixes() {
      return mFiles.parallelStream().map(B2FileVersion::getFileName)
          .filter(file -> file.endsWith(PATH_SEPARATOR)).collect(Collectors.toList())
          .toArray(new String[] {});
    }

    @Nullable @Override public ObjectListingChunk getNextChunk() throws IOException {
      return null;
    }
  }

  @Override public void setOwner(String path, String owner, String group) throws IOException {
  }

  @Override public void setMode(String path, short mode) throws IOException {
  }
}
