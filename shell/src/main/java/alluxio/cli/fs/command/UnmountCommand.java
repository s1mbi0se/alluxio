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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Unmounts an Alluxio path.
 */
@ThreadSafe
@PublicApi
public final class UnmountCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public UnmountCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "unmount";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  protected void runPlainPath(AlluxioURI inputPath, CommandLine cl)
      throws AlluxioException, IOException {
    mFileSystem.unmount(inputPath);
    System.out.println("Unmounted " + inputPath);
  }

  /**
   * Executes the {@code unmount} command.
   * <p>
   * Keeps all command line arguments stored in a collection.
   * The first argument should always correspond to the {@link AlluxioURI}
   * of the Alluxio path to be deleted.
   * <p>
   * Returns the exit code 0 if the operation is completed successfully.
   * Throws an exception otherwise.
   *
   * @param   cl  the parsed command line for the arguments
   * @return  the exit code 0 (zero) if the command runs successfully
   * @throws  IOException       if the provided URI does not exist or if
   *                            any other I/O-bound operation fails
   */
  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);
    runWildCardCmd(inputPath, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "unmount <alluxioPath>";
  }

  @Override
  public String getDescription() {
    return "Unmounts an Alluxio path.";
  }
}
