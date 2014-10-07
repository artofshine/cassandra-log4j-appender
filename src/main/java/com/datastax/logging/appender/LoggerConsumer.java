package com.datastax.logging.appender;

import com.codahale.metrics.Counter;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Joiner;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;

import java.util.UUID;
import java.util.concurrent.LinkedTransferQueue;

/**
 * Created by ikuchmin on 06.10.14.
 */
public class LoggerConsumer implements Runnable {
    private final LinkedTransferQueue<LoggingEvent> queueLogEvents;
    private final Session session;
    private final Counter queueLogs;
    // Параметры ниже можно перенести в сообщение очереди
    private final String appName;
    private final String ip;
    private final String hostname;
    private final String cfLogForDate;
    private final String cfLogForVLevel;

    public LoggerConsumer(LinkedTransferQueue<LoggingEvent> queueLogEvents, Session session, Counter queueLogs,
                          String appName, String ip, String hostname, String cfLogForDate, String cfLogForVLevel) {
        this.queueLogEvents = queueLogEvents;
        this.session = session;
        this.appName = appName;
        this.ip = ip;
        this.hostname = hostname;
        this.cfLogForDate = cfLogForDate;
        this.cfLogForVLevel = cfLogForVLevel;
        this.queueLogs = queueLogs;
    }

    @Override
    public void run() {
        while (true) {
            LoggingEvent event = null;
            try {
                event = queueLogEvents.take();
                queueLogs.dec();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (event != null) {
                createAndExecuteQuery(event);
            }
        }
    }

    /**
     * Send one logging event to Cassandra.  We just bind the new values into the preprocessed query
     * built by setupStatement
     * TODO: В случае если один из execute завершается с ошибкой, завершить процедуру без выброса Exception. Ситуацию можно воссоздать при RF = 1 и отключением любой из нод. Пример: Unexpected exception in the selector loop.java.lang.StackOverflowError
     * Проблема возникает еще и потому, что в случае ошибки Appender сыпит ошибки в общий лог, которые и пытается залогировать.
     */
    private void createAndExecuteQuery(LoggingEvent event)
    {
        LocationInfo locInfo = event.getLocationInformation();
        String className = null, fileName = null, lineNumber = null, methodName = null;
        if (locInfo != null) {
            className = locInfo.getClassName();
            fileName = locInfo.getFileName();
            lineNumber = locInfo.getLineNumber();
            methodName = locInfo.getMethodName();
        }

        String[] throwableStrs = event.getThrowableStrRep();
        String throwable = null;
        if (throwableStrs != null)
            throwable = Joiner.on(", ").join(throwableStrs);

        DateTime dateTime = new DateTime(event.getTimeStamp());
        long date = dateTime.withTime(0, 0, 0, 0).getMillis();

        {
            Batch batch = QueryBuilder.batch();
            Insert lfdQuery = QueryBuilder.insertInto(cfLogForDate)
                    .value(CassandraAppender.ID, UUID.randomUUID())
                    .value(CassandraAppender.APP_NAME, appName)
                    .value(CassandraAppender.HOST_IP, ip)
                    .value(CassandraAppender.HOST_NAME, hostname)
                    .value(CassandraAppender.LOGGER_NAME, event.getLoggerName())
                    .value(CassandraAppender.LEVEL, event.getLevel().toString())
                    .value(CassandraAppender.CLASS_NAME, className)
                    .value(CassandraAppender.FILE_NAME, fileName)
                    .value(CassandraAppender.LINE_NUMBER, lineNumber)
                    .value(CassandraAppender.METHOD_NAME, methodName)
                    .value(CassandraAppender.MESSAGE, event.getRenderedMessage())
                    .value(CassandraAppender.NDC, event.getNDC())
                    .value(CassandraAppender.APP_START_TIME, LoggingEvent.getStartTime())
                    .value(CassandraAppender.THREAD_NAME, event.getThreadName())
                    .value(CassandraAppender.THROWABLE_STR, throwable)
                    .value(CassandraAppender.TIMESTAMP, event.getTimeStamp())
                    .value(CassandraAppender.DATE, date);

            batch.add(lfdQuery);

            for (Level level : new Level[]{Level.TRACE, Level.DEBUG, Level.INFO,
                    Level.WARN, Level.ERROR, Level.FATAL, Level.ALL}) {
                if (event.getLevel().isGreaterOrEqual(level)){
                    Insert lfvQuery = QueryBuilder.insertInto(cfLogForVLevel)
                            .value(CassandraAppender.ID, UUID.randomUUID())
                            .value(CassandraAppender.APP_NAME, appName)
                            .value(CassandraAppender.HOST_IP, ip)
                            .value(CassandraAppender.HOST_NAME, hostname)
                            .value(CassandraAppender.LOGGER_NAME, event.getLoggerName())
                            .value(CassandraAppender.LEVEL, event.getLevel().toString())
                            .value(CassandraAppender.CLASS_NAME, className)
                            .value(CassandraAppender.FILE_NAME, fileName)
                            .value(CassandraAppender.LINE_NUMBER, lineNumber)
                            .value(CassandraAppender.METHOD_NAME, methodName)
                            .value(CassandraAppender.MESSAGE, event.getRenderedMessage())
                            .value(CassandraAppender.NDC, event.getNDC())
                            .value(CassandraAppender.APP_START_TIME, LoggingEvent.getStartTime())
                            .value(CassandraAppender.THREAD_NAME, event.getThreadName())
                            .value(CassandraAppender.THROWABLE_STR, throwable)
                            .value(CassandraAppender.TIMESTAMP, event.getTimeStamp())
                            .value(CassandraAppender.VLEVEL, level.toString());

                    batch.add(lfvQuery);
                }
            }
            session.executeAsync(batch);
//            try {
//                resultFutures.transfer(future);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }
}
