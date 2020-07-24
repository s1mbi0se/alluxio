package alluxio.logging.scope;

import alluxio.logging.scope.base.ScopeFlowLoggingAspect;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.util.UUID;

@Aspect
public class ScopeMasterFlowLoggingAspect extends ScopeFlowLoggingAspect {

    private static final String FLOW_NAME = "ScopeMasterFlow";
    private static final String START_METHOD = "execution(* alluxio.master.AlluxioMaster.main(..))";

    @Around(START_METHOD)
    public Object startFlux(final ProceedingJoinPoint point) throws Throwable {
        final long threadId = artificialThreadId;

        threadIdToStep.put(threadId, 0);
        threadIdToDebugLogId.compute(
                threadId, (key, value) -> UUID.randomUUID().getMostSignificantBits());

        return printDebugLogForMethod(point, threadId);
    }

    @Around(WHITE_AND_BLACK_LIST)
    public Object around(final ProceedingJoinPoint point) throws Throwable {

        return printDebugLogForMethod(point, artificialThreadId);
    }

    @Around(FINISH_METHOD)
    public Object finishFlux(final ProceedingJoinPoint point) throws Throwable {

        final Object resultFromMethod = printDebugLogForMethod(point, artificialThreadId);

        threadIdToStep.remove(artificialThreadId);
        threadIdToDebugLogId.remove(artificialThreadId);

        return resultFromMethod;
    }

    @Override
    protected String getFlowName() {
        return FLOW_NAME;
    }
}
