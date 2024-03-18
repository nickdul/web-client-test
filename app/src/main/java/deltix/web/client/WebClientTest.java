package deltix.web.client;

import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.epam.deltix.gflog.api.LogLevel;
import com.epam.deltix.gflog.core.LogConfig;
import com.epam.deltix.gflog.core.LogConfigurator;
import com.epam.deltix.gflog.core.Logger;
import com.epam.deltix.gflog.core.appender.Appender;
import com.epam.deltix.gflog.core.appender.ConsoleAppenderFactory;
import org.HdrHistogram.Histogram;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WebClientTest {
    static {
        System.setProperty("gflog.sync","true");
    }

    private final static Log LOG = LogFactory.getLog("KrakenfutHttpTest");

    private enum WebClientType {
        AHC, NETTY, OKHTTP, APACHE, JAVA
    }

    public static void main(String[] args) throws Exception {
        enableDetailedLog(Map.of("io.netty", LogLevel.DEBUG, LOG.getName(), LogLevel.DEBUG));

        final String apiKey = args[0];
        final String apiSecret = args[1];
        final WebClientType[] clientTypes = args.length > 2 ? parseClientTypes(args[2]) : new WebClientType[]{WebClientType.AHC};

        final int period = args.length > 3 ? Integer.parseInt(args[3]) : 200;
        final int batchCount = args.length > 4 ? Integer.parseInt(args[4]) : 20;
        final int requestInBatch = args.length > 5 ? Integer.parseInt(args[5]) : 5;
        final boolean useNativeTransport = args.length > 6 ? Boolean.parseBoolean(args[6]) : false;
        final int ioThreadCount = args.length > 7 ? Integer.parseInt(args[7]) : 0;

        LOG.info().append("Experiment Settings: ")
                .append("\n\tWeb Client Types: ").append(Arrays.toString(Arrays.stream(clientTypes).map(Enum::name).toArray(String[]::new)))
                .append("\n\tBatch Count: ").append(batchCount)
                .append("\n\tRequests in Batch: ").append(requestInBatch)
                .append("\n\tPeriod: ").append(period).append(" ms")
                .append("\n\tUse Native?: ").append(useNativeTransport)
                .append("\n\tIO Threads: ").append(ioThreadCount)
                .commit();

        final int warmupCount = 2;

        final CountDownLatch latch = new CountDownLatch(warmupCount *requestInBatch + batchCount * requestInBatch);

        for (WebClientType clientType : clientTypes) {
            try (WebClient httpRequestTest = createWebClient(clientType, apiKey, apiSecret, ioThreadCount, useNativeTransport, latch)) {
                doTest(httpRequestTest, latch, clientType, warmupCount, requestInBatch, batchCount, period);
            }
        }
    }

    private static void doTest(WebClient httpRequestTest, CountDownLatch latch, WebClientType clientType,
                               int warmupCount, int requestInBatch, int batchCount, int period) throws InterruptedException {
        final long maxDuration = 100 * 1_000_000L;
        final Histogram histogram = new Histogram(maxDuration, 3);

        for (int i = 0; i < warmupCount; i++) {
            for (int j = 0; j < requestInBatch; j++) {
                httpRequestTest.executeRequest();
            }
            Thread.sleep(100);
        }

        Thread.sleep(2000);

        LOG.info("\n\n");
        LOG.info("(%s) Start Experiment").with(clientType);
        for (int i = 0; i < batchCount; i++) {
            if (i > 0) {
                Thread.sleep(period);
            }

            for (int j = 0; j < requestInBatch; j++) {
                final long startTime = System.nanoTime();

                httpRequestTest.executeRequest();

                final long duration = System.nanoTime() - startTime;
                if (duration < maxDuration) {
                    histogram.recordValue(duration);
                }
            }
        }
        LOG.info("(%s) Finish Experiment").with(clientType);
        LOG.info("(%s) Responses are not received so far: %s").with(clientType).with(latch.getCount());

        LOG.info("(%s) Balance Requests Stats: \n%s").with(clientType).with(toString(histogram));

        if (latch.await(2, TimeUnit.MINUTES)) {
            LOG.info("(%s) All responses successfully received.\n\n").with(clientType);
        } else {
            LOG.error("(%s) %s responses still not received!\n\n").with(clientType).with(latch.getCount());
        }
    }

    private static WebClientType[] parseClientTypes(String value) {
        final String[] types = value.split(",");
        final WebClientType[] result = new WebClientType[types.length];
        for (int i = 0; i < types.length; i++) {
            result[i] = WebClientType.valueOf(types[i]);
        }
        return result;
    }

    private static WebClient createWebClient(WebClientType testType, String apiKey, String apiSecret,
                                             int ioThreadCount, boolean useNativeTransport,
                                             CountDownLatch latch) throws Exception {
        final WebClient httpRequestTest;
        switch (testType) {
            case NETTY:
                httpRequestTest = KrakenfutUtil.createNettyClient(ioThreadCount, useNativeTransport, apiKey, apiSecret, latch, LOG);
                break;
            case OKHTTP:
                httpRequestTest = KrakenfutUtil.createOkhttpClient(ioThreadCount, apiKey, apiSecret, latch, LOG);
                break;
            case JAVA:
                httpRequestTest = KrakenfutUtil.createJavaClient(ioThreadCount, apiKey, apiSecret, latch, LOG);
                break;
            case APACHE:
                httpRequestTest = KrakenfutUtil.createApacheClient(apiKey, apiSecret, latch, LOG);
                break;
            default:
                httpRequestTest = KrakenfutUtil.createAhcClient(ioThreadCount, useNativeTransport, apiKey, apiSecret, latch, LOG);
        }
        return httpRequestTest;
    }

    private static final double[] PERCENTILES = new double[]{
            0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 99.0, 99.9, 99.99, 99.999, 100.0
    };

    public static String toString(Histogram histogram) {
        final StringBuilder builder = new StringBuilder(256);
        builder.append(String.format("\tPercentile (%%)     Value (ns)          Count      Count Sum%n"));

        long prevCount = 0;
        for (double percentile : PERCENTILES) {
            long valueAtPercentile = histogram.getValueAtPercentile(percentile);
            long cumulativeCount = histogram.getCountBetweenValues(0, valueAtPercentile);
            long count = cumulativeCount - prevCount;
            builder.append(String.format("\t       %7.3f      %,9d      %9s      %9s%n", percentile, valueAtPercentile, count, cumulativeCount));
            prevCount = cumulativeCount;
        }
        builder.append(String.format("\t[Min %s ns, Mean %.0f ns, Max %s ns, Total %s]", histogram.getMinValue(), histogram.getMean(), histogram.getMaxValue(), histogram.getTotalCount()));
        return builder.toString();
    }

    public static void enableDetailedLog(Map<String, LogLevel> loggers){
        final ConsoleAppenderFactory consoleAppenderFactory = new ConsoleAppenderFactory();
        consoleAppenderFactory.setName("console");
        final Appender consoleAppender = consoleAppenderFactory.create();

        final LogConfig logConfig = new LogConfig();
        loggers.forEach((key, value) -> logConfig.addLogger(new Logger(key, value, consoleAppender)));

        logConfig.addAppender(consoleAppender);
        LogConfigurator.configure(logConfig);
    }
}
