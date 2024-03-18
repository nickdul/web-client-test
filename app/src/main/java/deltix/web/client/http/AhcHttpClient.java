package deltix.web.client.http;

import com.epam.deltix.gflog.api.Log;
import deltix.web.client.WebClient;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.asynchttpclient.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

public final class AhcHttpClient implements WebClient {
    private final AsyncHttpClient client;
    private final Request request;
    private final BiFunction<Response, Throwable, Response> responseHandler;

    public AhcHttpClient(int ioThreadCount, boolean useNativeTransport,
                         RequestBuilder request,
                         BiFunction<Response, Throwable, Response> responseHandler) throws Exception {
        this.request = request.build();
        this.responseHandler = responseHandler;

        final SslContext sslContext = SslContextBuilder.forClient()
                .protocols("TLSv1.3")
                .build();

        final DefaultAsyncHttpClientConfig.Builder httpClientConfig = Dsl.config();
        httpClientConfig.setThreadPoolName("KRAKEN");
        httpClientConfig.setKeepAlive(true);
        httpClientConfig.setTcpNoDelay(true);
        httpClientConfig.setMaxRequestRetry(0);

        httpClientConfig.setCookieStore(null);
        httpClientConfig.setValidateResponseHeaders(false);
        httpClientConfig.setDisableUrlEncodingForBoundRequests(true);

        httpClientConfig.setSslContext(sslContext);
        httpClientConfig.setUseNativeTransport(useNativeTransport);

        //httpClientConfig.setMaxConnections(512); // by default - 200
        //httpClientConfig.setMaxConnectionsPerHost(512); // by default - unlimited

        if (ioThreadCount > 0) {
            httpClientConfig.setIoThreadsCount(ioThreadCount);
        }

        client = Dsl.asyncHttpClient(httpClientConfig);
    }

    @Override
    public void executeRequest() {
        client.executeRequest(request).toCompletableFuture().handle(responseHandler);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    public static class AhcResponseHandler implements BiFunction<Response, Throwable, Response> {
        private final CountDownLatch latch;
        private final Log logger;

        public AhcResponseHandler(CountDownLatch latch, Log logger) {
            this.latch = latch;
            this.logger = logger;
        }

        @Override
        public Response apply(Response response, Throwable throwable) {
            latch.countDown();

            if (throwable != null) {
                logger.error().append(throwable).commit();
                return response;
            }
            if (response.getStatusCode() != 200) {
                logger.error("Response is not successful: %s").with(response.getStatusCode());
            }
            return response;
        }
    }
}
