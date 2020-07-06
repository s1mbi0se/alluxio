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
import alluxio.cli.fsadmin.report.UfsCommand;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.MountPOptions;
import alluxio.wire.MountPointInfo;

import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Mounts a UFS path onto an Alluxio path.
 */
@ThreadSafe
@PublicApi
public final class MountCommand extends AbstractFileSystemCommand {

  private static final Option READONLY_OPTION =
      Option.builder()
          .longOpt("readonly")
          .required(false)
          .hasArg(false)
          .desc("mount point is readonly in Alluxio")
          .build();
  private static final Option SHARED_OPTION =
      Option.builder()
          .longOpt("shared")
          .required(false)
          .hasArg(false)
          .desc("mount point is shared")
          .build();
  private static final Option OPTION_OPTION =
      Option.builder()
          .longOpt("option")
          .required(false)
          .hasArg(true)
          .numberOfArgs(2)
          .argName("key=value")
          .valueSeparator('=')
          .desc("options associated with this mount point")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public MountCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "mount";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(READONLY_OPTION).addOption(SHARED_OPTION)
        .addOption(OPTION_OPTION);
  }

  /**
   * Executes the {@code mount} command.
   * <p>
   * Keeps all command line arguments stored in a collection. Checks
   * if this collection is empty, in which case information about the
   * mount table with all actively synced paths is printed and the exit
   * code 0 is returned. If the collection is not empty, this step is
   * skipped.
   * <p>
   * The first command line argument corresponds to the Alluxio path,
   * and the second one corresponds to the under file storage path.
   * <p>
   * Implements {@link #READONLY_OPTION} and/or {@link #SHARED_OPTION}
   * for the new mount point, if there are flags enabling these options
   * in the command line. All options are saved to a
   * {@link MountPOptions.Builder}.
   * <p>
   * Creates a new mount point to the {@link #mFileSystem} with
   * {@link alluxio.client.file.FileSystem#mount(AlluxioURI, AlluxioURI, MountPOptions)},
   * returning the exit code 0, which indicates the operation was executed successfully.
   *
   * @param   cl  the parsed command line for the arguments
   * @return  the exit code 0 (zero) if the command runs successfully
   * @throws  AlluxioException  if an exception occurs while trying to
   *                            get information about the mount table.
   * @throws  IOException       if the command returns a non-zero value, which
   *                            means the command could run successfully
   */
  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    if (args.length == 0) {
      Map<String, MountPointInfo> mountTable = mFileSystem.getMountTable();
      UfsCommand.printMountInfo(mountTable);
      return 0;
    }
    AlluxioURI alluxioPath = new AlluxioURI(args[0]);
    AlluxioURI ufsPath = new AlluxioURI(args[1]);
    MountPOptions.Builder optionsBuilder = MountPOptions.newBuilder();

    if (cl.hasOption(READONLY_OPTION.getLongOpt())) {
      optionsBuilder.setReadOnly(true);
    }
    if (cl.hasOption(SHARED_OPTION.getLongOpt())) {
      optionsBuilder.setShared(true);
    }
    if (cl.hasOption(OPTION_OPTION.getLongOpt())) {
      Properties properties = cl.getOptionProperties(OPTION_OPTION.getLongOpt());
      optionsBuilder.putAllProperties(Maps.fromProperties(properties));
    }
    mFileSystem.mount(alluxioPath, ufsPath, optionsBuilder.build());
    System.out.println("Mounted " + ufsPath + " at " + alluxioPath);
    return 0;
  }

  @Override
  public String getUsage() {
    return "mount [--readonly] [--shared] [--option <key=val>] <alluxioPath> <ufsURI>";
  }

  @Override
  public String getDescription() {
    return "Mounts a UFS path onto an Alluxio path.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (cl.getArgs().length != 2 && cl.getArgs().length != 0) {
      throw new InvalidArgumentException("Command mount takes 0 or 2 arguments, not " + cl
          .getArgs().length);
    }
  }
}
