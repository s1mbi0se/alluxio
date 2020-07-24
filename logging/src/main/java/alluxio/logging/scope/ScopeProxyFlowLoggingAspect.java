package alluxio.logging.scope;

import alluxio.logging.scope.base.ScopeFlowLoggingAspect;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

/**
 * This class represents a {@link alluxio.logging.base.FlowLoggingAspect} for the
 * Alluxio Proxy, logging and keeping track of all method calls it executes at any
 * given moment.
 */
@Aspect
public final class ScopeProxyFlowLoggingAspect extends ScopeFlowLoggingAspect {

    private static final String FLOW_NAME = "ScopeProxyFlow";
    private static final String START_METHOD = "execution(* alluxio.proxy.AlluxioProxy.main(..))";

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
