package com.datastax.logging.appender;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by ikuchmin on 06.10.14.
 */
public class FutureAnalyzer implements Runnable {
    private final LinkedTransferQueue<ResultSetFuture> resultFutures;

    public FutureAnalyzer(LinkedTransferQueue<ResultSetFuture> resultFutures) {
        this.resultFutures = resultFutures;
    }

    @Override
    public void run() {
        while (true) {
            try {
                ResultSetFuture future = resultFutures.take();
                future.getUninterruptibly(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
//                e.printStackTrace();
            } catch (NoHostAvailableException ha) {
//                ha.printStackTrace();
            } catch (WriteTimeoutException wt) {
//                wt.printStackTrace();
            } catch (QueryExecutionException qe) {
//                qe.printStackTrace();
            } catch (QueryValidationException qv) {
//                qv.printStackTrace();
            } catch (Exception e) {
//                e.printStackTrace();
            }

        }
    }
}
