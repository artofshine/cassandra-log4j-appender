package com.datastax.logging.appender;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;

import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import org.codehaus.jackson.map.ObjectMapper;

import com.datastax.driver.core.policies.RoundRobinPolicy;

import com.google.common.base.Joiner;
import org.joda.time.DateTime;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Main class that uses Cassandra to store log entries into.
 *
 */
public class CassandraAppender extends AppenderSkeleton
{
    // Cassandra configuration
    private String hosts = "localhost";
    private int port = 9042; //for the binary pr    otocol, 9160 is default for thrift
    private String username = "";
    private String password = "";
    private String ip = getLocalIP();
    private String hostname = getLocalHostName();

    // Encryption.  sslOptions and authProviderOptions are JSON maps requiring Jackson
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private Map<String, String> sslOptions = null;
    private Map<String, String> authProviderOptions = null;

    // Keyspace/ColumnFamily information
    private String keyspaceName = "Logging";
	private String cfLogForDate = "log_entries_date";
    private String cfLogForVLevel = "log_entries_vlevel";
	private String appName = "default";
    private String replication = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
    private ConsistencyLevel consistencyLevelWrite = ConsistencyLevel.ONE;

    // CF column names
    public static final String ID = "key";
    public static final String DATE = "date";
    public static final String HOST_IP = "host_ip";
    public static final String HOST_NAME = "host_name";
    public static final String APP_NAME = "app_name";
    public static final String LOGGER_NAME = "logger_name";
    public static final String LEVEL = "level";
    public static final String VLEVEL = "vlevel";
    public static final String CLASS_NAME = "class_name";
    public static final String FILE_NAME = "file_name";
    public static final String LINE_NUMBER = "line_number";
    public static final String METHOD_NAME = "method_name";
    public static final String MESSAGE = "message";
    public static final String NDC = "ndc";
    public static final String APP_START_TIME = "app_start_time";
    public static final String THREAD_NAME = "thread_name";
    public static final String THROWABLE_STR = "throwable_str_rep";
    public static final String TIMESTAMP = "log_timestamp";

    // session state
    private PreparedStatement statement;
    private volatile boolean initialized = false;
    private volatile boolean initializationFailed = false;
    private Cluster cluster;
    private Session session;

    // perfomance state
    private int connPerHost = 20;
    private int countLoggerConsumers = 40;

    // async tuning
    LinkedTransferQueue<ResultSetFuture> resultFutures;
    LinkedTransferQueue<LoggingEvent> queueLogEvents;

    // metrics
    public static final MetricRegistry metrics = new MetricRegistry();
    private Counter brokenLogs;
    private Counter attemptLogs;
    private Counter queueLogs;

    public CassandraAppender()
    {
        brokenLogs = metrics.counter(name(CassandraAppender.class, "BrokenLogsCount"));
        attemptLogs = metrics.counter(name(CassandraAppender.class, "AttemptLogsCount"));
        queueLogs = metrics.counter(name(CassandraAppender.class, "QueueLogsCount"));

        final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics).build();
        jmxReporter.start();

		LogLog.debug("Creating CassandraAppender");
	}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void append(LoggingEvent event)
    {
        // We have to defer initialization of the client because TTransportFactory
        // references some Hadoop classes which can't safely be used until the logging
        // infrastructure is fully set up. If we attempt to initialize the client
        // earlier, it causes NPE's from the constructor of org.apache.hadoop.conf.Configuration.

        if (!initialized)
            initClient();
        if (!initializationFailed) {
            queueLogEvents.put(event);
            queueLogs.inc();
        }
//            if (queueLogEvents.tryTransfer(event))
//                attemptLogs.inc();
//            else
//                brokenLogs.inc();
    }

    //Connect to cassandra, then setup the schema and preprocessed statement
    protected synchronized void initClient()
    {
        // We should be able to go without an Atomic variable here.  There are two potential problems:
        // 1. Multiple threads read intialized=false and call init client.  However, the method is
        //    synchronized so only one will get the lock first, and the others will drop out here.
        // 2. One thread reads initialized=true before initClient finishes.  This also should not
        //    happen as the lock should include a memory barrier.
        if (initialized || initializationFailed)
            return;

		// Just while we initialise the client, we must temporarily
		// disable all logging or else we get into an infinite loop
		Level globalThreshold = LogManager.getLoggerRepository().getThreshold();
		LogManager.getLoggerRepository().setThreshold(Level.OFF);

		try
        {
            Cluster.Builder builder = Cluster.builder()
                                             .addContactPoints(hosts.split(",s*"))
                                             .withPort(port)
                                             .withLoadBalancingPolicy(new RoundRobinPolicy())
                                             .withPoolingOptions(
                                                     new PoolingOptions()
                                                             .setMaxConnectionsPerHost(HostDistance.LOCAL, connPerHost)
                                                             .setCoreConnectionsPerHost(HostDistance.LOCAL, connPerHost));

            // Kerberos provides authentication anyway, so a username and password are superfluous.  SSL
            // is compatible with either.
            boolean passwordAuthentication = !password.equals("") || !username.equals("");
            if (authProviderOptions != null && passwordAuthentication)
                throw new IllegalArgumentException("Authentication via both Cassandra usernames and Kerberos " +
                                                   "requested.");

            // Encryption
            if (authProviderOptions != null)
                builder = builder.withAuthProvider(getAuthProvider());
            if (sslOptions != null)
                builder = builder.withSSL(getSslOptions());
            if (passwordAuthentication)
                builder = builder.withCredentials(username, password);

            cluster = builder.build();
            //TODO: Проблема курицы и яйца. При первом запуске keyspace не существует, а подключение не создается чтобы создать его далее
		    session = cluster.connect(keyspaceName);
            setupSchema();
            setupAsync();
        }
        catch (Exception e)
        {
		    LogLog.error("Error ", e);
			errorHandler.error("Error setting up cassandra logging schema: " + e);

            //If the user misconfigures the port or something, don't keep failing.
            initializationFailed = true;
		}
        finally
        {
            //Always reenable logging
            LogManager.getLoggerRepository().setThreshold(globalThreshold);
            initialized = true;
		}
	}


    /**
     * Initialize queue and consumers for async execution
     */
    private void setupAsync() {
        queueLogEvents = new LinkedTransferQueue<LoggingEvent>();
        ExecutorService loggerConsumers = Executors.newFixedThreadPool(countLoggerConsumers);
        for (int i = 0; i < countLoggerConsumers; i++)
            loggerConsumers.execute(new LoggerConsumer(queueLogEvents, session, queueLogs,
                    appName, ip, hostname, cfLogForDate, cfLogForVLevel));

//        resultFutures = new LinkedTransferQueue<ResultSetFuture>();
//        ExecutorService fAnalyzePool= Executors.newFixedThreadPool(connPerHost * 9);
//        for (int i = 0; i < connPerHost * 9; i++) {
//            fAnalyzePool.execute(new FutureAnalyzer(resultFutures));
//        }
    }

    /**
     * Create Keyspace and CF if they do not exist.
     */
    private void setupSchema() throws IOException
    {
        {
            //Create keyspace if necessary
            String query = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s;",
                    keyspaceName, replication);
            session.execute(query);
        }

        {
            String primaryKeyCols = String.format("%s, %s, %s", HOST_NAME, APP_NAME, DATE);
            String clusterKeyCols = String.format("%s, %s", TIMESTAMP, ID);

            //Create table if necessary pure log
            String query = String.format("CREATE TABLE IF NOT EXISTS %s (%s UUID, " +
                            "%s text, %s bigint, %s text, %s text, %s text, %s text, %s text," +
                            "%s text, %s text, %s bigint, %s text, %s text, %s text, %s text," +
                            "%s text, %s bigint, PRIMARY KEY ((%s), %s)) WITH CLUSTERING ORDER BY" +
                            "(%s DESC);",
                    cfLogForDate, ID, APP_NAME, APP_START_TIME, CLASS_NAME,
                    FILE_NAME, HOST_IP, HOST_NAME, LEVEL, LINE_NUMBER, METHOD_NAME,
                    TIMESTAMP, LOGGER_NAME, MESSAGE, NDC, THREAD_NAME, THROWABLE_STR, DATE,
                    primaryKeyCols, clusterKeyCols, TIMESTAMP);
            session.execute(query);
        }

        {
            String primaryKeyCols = String.format("%s, %s, %s", HOST_NAME, APP_NAME, VLEVEL);
            String clusterKeyCols = String.format("%s, %s", TIMESTAMP, ID);
            //Create table if necessary mixes level
            String query = String.format("CREATE TABLE IF NOT EXISTS %s (%s UUID, " +
                            "%s text, %s bigint, %s text, %s text, %s text, %s text, %s text," +
                            "%s text, %s text, %s bigint, %s text, %s text, %s text, %s text," +
                            "%s text, %s text, PRIMARY KEY ((%s), %s)) WITH CLUSTERING ORDER BY" +
                            "(%s DESC);",
                    cfLogForVLevel, ID, APP_NAME, APP_START_TIME, CLASS_NAME,
                    FILE_NAME, HOST_IP, HOST_NAME, LEVEL, LINE_NUMBER, METHOD_NAME,
                    TIMESTAMP, LOGGER_NAME, MESSAGE, NDC, THREAD_NAME, THROWABLE_STR, VLEVEL,
                    primaryKeyCols, clusterKeyCols, TIMESTAMP);
            session.execute(query);
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
                    .value(ID, UUID.randomUUID())
                    .value(APP_NAME, appName)
                    .value(HOST_IP, ip)
                    .value(HOST_NAME, hostname)
                    .value(LOGGER_NAME, event.getLoggerName())
                    .value(LEVEL, event.getLevel().toString())
                    .value(CLASS_NAME, className)
                    .value(FILE_NAME, fileName)
                    .value(LINE_NUMBER, lineNumber)
                    .value(METHOD_NAME, methodName)
                    .value(MESSAGE, event.getRenderedMessage())
                    .value(NDC, event.getNDC())
                    .value(APP_START_TIME, LoggingEvent.getStartTime())
                    .value(THREAD_NAME, event.getThreadName())
                    .value(THROWABLE_STR, throwable)
                    .value(TIMESTAMP, event.getTimeStamp())
                    .value(DATE, date);

            batch.add(lfdQuery);

            for (Level level : new Level[]{Level.TRACE, Level.DEBUG, Level.INFO,
                    Level.WARN, Level.ERROR, Level.FATAL, Level.ALL}) {
                if (event.getLevel().isGreaterOrEqual(level)){
                    Insert lfvQuery = QueryBuilder.insertInto(cfLogForVLevel)
                            .value(ID, UUID.randomUUID())
                            .value(APP_NAME, appName)
                            .value(HOST_IP, ip)
                            .value(HOST_NAME, hostname)
                            .value(LOGGER_NAME, event.getLoggerName())
                            .value(LEVEL, event.getLevel().toString())
                            .value(CLASS_NAME, className)
                            .value(FILE_NAME, fileName)
                            .value(LINE_NUMBER, lineNumber)
                            .value(METHOD_NAME, methodName)
                            .value(MESSAGE, event.getRenderedMessage())
                            .value(NDC, event.getNDC())
                            .value(APP_START_TIME, LoggingEvent.getStartTime())
                            .value(THREAD_NAME, event.getThreadName())
                            .value(THROWABLE_STR, throwable)
                            .value(TIMESTAMP, event.getTimeStamp())
                            .value(VLEVEL, level.toString());

                    batch.add(lfvQuery);
                }
            }
            ResultSetFuture future = null;
            try {
                future = session.executeAsync(batch);
            } catch (Exception e) {
            }
            try {
                resultFutures.transfer(future);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        session.closeAsync();
        cluster.closeAsync();
    }

    /**
     * {@inheritDoc}
     *
     * @see org.apache.log4j.Appender#requiresLayout()
     */
    public boolean requiresLayout()
    {
        return false;
    }

    /**
     * Called once all the options have been set. Starts listening for clients
     * on the specified socket.
     */
    public void activateOptions()
    {
        // reset();
    }

    //
    //Boilerplate from here on out
    //

    public String getKeyspaceName()
    {
		return keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName)
    {
		this.keyspaceName = keyspaceName;
	}

	public String keyosts()
    {
		return hosts;
	}

	public void setHosts(String hosts)
    {
		this.hosts = hosts;
	}

	public int getPort()
    {
		return port;
	}

	public void setPort(int port)
    {
		this.port = port;
	}

	public String getUsername()
    {
		return username;
	}

	public void setUsername(String username)
    {
		this.username = unescape(username);
	}

	public String getPassword()
    {
		return password;
	}

	public void setPassword(String password)
    {
		this.password = unescape(password);
	}

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getCfLogForDate() {
        return cfLogForDate;
    }

    public void setCfLogForDate(String cfLogForDate) {
        this.cfLogForDate = cfLogForDate;
    }

    public String getCfLogForVLevel() {
        return cfLogForVLevel;
    }

    public void setCfLogForVLevel(String cfLogForVLevel) {
        this.cfLogForVLevel = cfLogForVLevel;
    }

    public String getReplication()
    {
		return replication;
	}

	public void setReplication(String strategy)
    {
		replication = unescape(strategy);
	}

    private Map<String, String> parseJsonMap(String options, String type) throws Exception
    {
        if (options == null)
            throw new IllegalArgumentException(type + "Options can't be null.");

        return jsonMapper.readValue(unescape(options), new TreeMap<String, String>().getClass());
    }

    public void setAuthProviderOptions(String newOptions) throws Exception
    {
        authProviderOptions = parseJsonMap(newOptions, "authProvider");
    }

    public void setSslOptions(String newOptions) throws Exception
    {
        sslOptions = parseJsonMap(newOptions, "Ssl");
    }

	public String getConsistencyLevelWrite()
    {
		return consistencyLevelWrite.toString();
	}

	public void setConsistencyLevelWrite(String consistencyLevelWrite)
    {
		try {
			this.consistencyLevelWrite = ConsistencyLevel.valueOf(unescape(consistencyLevelWrite));
		}
        catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Consistency level " + consistencyLevelWrite
					+ " wasn't found. Available levels: " + Joiner.on(", ").join(ConsistencyLevel.values()));
		}
	}


    public String getAppName()
    {
		return appName;
	}

	public void setAppName(String appName)
    {
		this.appName = appName;
	}

    public int getConnPerHost() {
        return connPerHost;
    }

    public void setConnPerHost(int connPerHost) {
        this.connPerHost = connPerHost;
    }

    private static String getLocalHostName()
    {
		String hostname = "unknown";

		try {
			InetAddress addr = InetAddress.getLocalHost();
			hostname = addr.getHostName();
		} catch (Throwable t) {

		}
		return hostname;
	}

	private static String getLocalIP()
    {
		String ip = "unknown";

		try {
			InetAddress addr = InetAddress.getLocalHost();
			ip = addr.getHostAddress();
		} catch (Throwable t) {

		}
		return ip;
	}

	/**
	 * Strips leading and trailing '"' characters
	 *
	 * @param b
	 *            - string to unescape
	 * @return String - unexspaced string
	 */
	private static String unescape(String b)
    {
		if (b.charAt(0) == '\"' && b.charAt(b.length() - 1) == '\"')
			b = b.substring(1, b.length() - 1);
		return b;
	}

    // Create an SSLContext (a container for a keystore and a truststore and their associated options)
    // Assumes sslOptions map is not null
    private SSLOptions getSslOptions() throws Exception
    {
        // init trust store
        TrustManagerFactory tmf = null;
        String truststorePath = sslOptions.get("ssl.truststore");
        String truststorePassword = sslOptions.get("ssl.truststore.password");
        if (truststorePath != null && truststorePassword != null)
        {
            FileInputStream tsf = new FileInputStream(truststorePath);
            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(tsf, truststorePassword.toCharArray());
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
        }

        // init key store
        KeyManagerFactory kmf = null;
        String keystorePath = sslOptions.get("ssl.keystore");
        String keystorePassword = sslOptions.get("ssl.keystore.password");
        if (keystorePath != null && keystorePassword != null)
        {
            FileInputStream ksf = new FileInputStream(keystorePath);
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksf, keystorePassword.toCharArray());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keystorePassword.toCharArray());

        }

        // init cipher suites
        String[] ciphers = SSLOptions.DEFAULT_SSL_CIPHER_SUITES;
        if (sslOptions.containsKey("ssl.ciphersuites"))
            ciphers = sslOptions.get("ssl.ciphersuits").split(",\\s*");

        SSLContext ctx = SSLContext.getInstance("SSL");
        ctx.init(kmf == null ? null : kmf.getKeyManagers(),
                 tmf == null ? null : tmf.getTrustManagers(),
                 new SecureRandom());

        return new SSLOptions(ctx, ciphers);
    }

    // Load a custom AuthProvider class dynamically.
    public AuthProvider getAuthProvider() throws Exception
    {
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        if(!authProviderOptions.containsKey("auth.class"))
            throw new IllegalArgumentException("authProvider map does not include auth.class.");
        Class dap = cl.loadClass(authProviderOptions.get("auth.class"));

        // Perhaps this should be a factory, but it seems easy enough to just have a single string parameter
        // which can be encoded however, e.g. another JSON map
        if(authProviderOptions.containsKey("auth.options"))
            return (AuthProvider)dap.getConstructor(String.class).newInstance(authProviderOptions.get("auth.options"));
        else
            return (AuthProvider)dap.newInstance();
    }
}

