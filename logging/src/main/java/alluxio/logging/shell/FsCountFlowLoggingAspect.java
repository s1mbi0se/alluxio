package alluxio.logging.shell;

import alluxio.logging.shell.base.FsFlowLoggingAspect;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

/**
 * This class represents a {@link alluxio.logging.base.FlowLoggingAspect} for the
 * file system command {@code count}. All methods triggered by this command are logged
 * and kept track of.
 */
@Aspect
public final class FsCountFlowLoggingAspect extends FsFlowLoggingAspect {

    private static final String FLOW_NAME = "FsCountFlow";
    private static final String START_METHOD = "execution(* alluxio.cli.fs.command.CountCommand.run(..))";

    @Around(START_METHOD)
    @Override
    public Object startFlow(final ProceedingJoinPoint point) throws Throwable {
        return super.startFlow(point);
    }

    @Around(WHITE_AND_BLACK_LIST)
    @Override
    public Object around(final ProceedingJoinPoint point) throws Throwable {
        return super.around(point);
    }

    @Around(FINISH_METHOD)
    @Override
    public Object finishFlow(final ProceedingJoinPoint point) throws Throwable {
        return super.finishFlow(point);
    }

    @Override
    protected String getFlowName() {
        return FLOW_NAME;
    }
}
