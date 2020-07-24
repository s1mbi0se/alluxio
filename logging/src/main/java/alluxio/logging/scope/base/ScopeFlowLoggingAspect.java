package alluxio.logging.scope.base;

import br.com.simbiose.debug_log.BaseAspect;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Aspect
public abstract class ScopeFlowLoggingAspect extends BaseAspect {
    private static final String WHITE_AND_BLACK_LIST = "";
    private static final String FINISH_METHOD = "";

    protected final Map<Long, Integer> threadIdToStep = new ConcurrentHashMap<>();
    protected final Map<Long, Long> threadIdToDebugLogId = new ConcurrentHashMap<>();

    @Around(WHITE_AND_BLACK_LIST)
    public Object around(final ProceedingJoinPoint point) throws Throwable {
        final long threadId = Thread.currentThread().getId();

        return printDebugLogForMethod(point, threadId);
    }

    @Around(FINISH_METHOD)
    public Object finishFlux(final ProceedingJoinPoint point) throws Throwable {
        final long threadId = Thread.currentThread().getId();

        final Object resultFromMethod = printDebugLogForMethod(point, threadId);

        threadIdToStep.remove(threadId);
        threadIdToDebugLogId.remove(threadId);

        return resultFromMethod;
    }

    @Override
    protected Map<Long, Integer> getThreadIdToStep() {
        return this.threadIdToStep;
    }

    @Override
    protected Map<Long, Long> getThreadIdToDebugLogId() {
        return this.threadIdToDebugLogId;
    }
}