package alluxio.logging.shell.base;

import alluxio.logging.base.FlowLoggingAspect;

/**
 * This class represents every logging aspect for Alluxio file system commands,
 * such as mount, unmount, mkdir, mv, rm, etc.
 * <p>
 * This class is used to avoid breaking the D.R.Y. principle - all other logging
 * aspect classes extending this share the same {@link #WHITE_AND_BLACK_LIST} and
 * {@link #FINISH_METHOD}.
 */
public abstract class FsFlowLoggingAspect extends FlowLoggingAspect {
    // A string representing a list used to determine which methods from
    // within a given range should be logged or not
    protected static final String WHITE_AND_BLACK_LIST = "execution(* alluxio..*(..)) && "
            + "!within(alluxio.logging..*) && "
            + "!within(alluxio.clock..*) && "
            + "!within(alluxio.client.metrics..*) && "
            + "!within(alluxio.heartbeat..*) && "
            + "!within(alluxio.resource..*) && "
            + "!within(alluxio.time..*) && "
            + "!within(alluxio.conf..*) && "
            + "!within(br.com.simbiose..*) && "
            + "!within(java..*)";

    // A string representing the condition/method that needs to be called for this logging
    // aspect to halt its execution
    protected static final String FINISH_METHOD = "execution(* java.lang.System.exit(..))";
}
