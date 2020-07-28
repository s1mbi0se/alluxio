package alluxio.logging.base;

import br.com.simbiose.debug_log.BaseAspect;
import org.aspectj.lang.ProceedingJoinPoint;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A base class for all flow logging aspects. This class defines basic
 * logging aspect operations needed for {@link BaseAspect} to successfully
 * log the desired flows.
 */
public abstract class FlowLoggingAspect extends BaseAspect {

    /*
    A fake thread ID needed for a flow not to be interrupted
    in between threads with different IDs
     */
    protected static final long artificialThreadId = -1L;

    // Keeps the ID of the thread currently executing a method to be logged
    protected final Map<Long, Integer> threadIdToStep = new ConcurrentHashMap<>();

    // Maps thread ID to flow ID
    protected final Map<Long, Long> threadIdToDebugLogId = new ConcurrentHashMap<>();

    /**
     * Starts logging all methods within the white/blacklist once the initial method is called.
     *
     * @param point the {@link ProceedingJoinPoint} for the {@code proceed(..)} method
     *        in AspectJ
     * @return the same object of the method being wrapped by AspectJ
     * @throws Throwable the same {@code Throwable} thrown bt the method
     *         being wrapped by AspectJ
     */
    public Object startFlow(final ProceedingJoinPoint point) throws Throwable {
        final long threadId = artificialThreadId;

        threadIdToStep.put(threadId, 0);
        threadIdToDebugLogId.compute(
                threadId, (key, value) -> UUID.randomUUID().getMostSignificantBits());

        return printDebugLogForMethod(point, threadId);
    }

    /**
     * Retrieves the thread ID and delegates it to {@link BaseAspect}
     *
     * @param point the {@link ProceedingJoinPoint} for the {@code proceed(..)} method
     *        in AspectJ
     * @return the same object of the method being wrapped by AspectJ
     * @throws Throwable the same {@code Throwable} thrown bt the method
     *         being wrapped by AspectJ
     */
    public Object around(final ProceedingJoinPoint point) throws Throwable {

        return printDebugLogForMethod(point, artificialThreadId);
    }

    /**
     * Stops logging once the last method is executed.
     *
     * @param point the {@link ProceedingJoinPoint} for the {@code proceed(..)} method
     *        in AspectJ
     * @return the same object of the method being wrapped by AspectJ
     * @throws Throwable the same {@code Throwable} thrown bt the method
     *         being wrapped by AspectJ
     */
    public Object finishFlow(final ProceedingJoinPoint point) throws Throwable {

        final Object resultFromMethod = printDebugLogForMethod(point, artificialThreadId);

        threadIdToStep.remove(artificialThreadId);
        threadIdToDebugLogId.remove(artificialThreadId);

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
