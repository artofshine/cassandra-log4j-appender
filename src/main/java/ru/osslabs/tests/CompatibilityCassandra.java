package ru.osslabs.tests;

import com.datastax.logging.appender.CassandraAppender;

/**
 * Created by ikuchmin on 25.09.14.
 */
public class CompatibilityCassandra extends CassandraAppender{
    public static void main(String[] args) {
        CompatibilityCassandra appender = new CompatibilityCassandra();
        appender.setHosts("192.168.98.171");
        appender.initClient();
    }
}
