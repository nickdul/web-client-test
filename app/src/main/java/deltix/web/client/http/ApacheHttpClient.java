package deltix.web.client.http;

import com.epam.deltix.gflog.api.Log;
import deltix.web.client.WebClient;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.utils.URIUtils;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public final class ApacheHttpClient implements WebClient {
    private static final HttpContext CONTEXT = HttpClientContext.create();

    private final CloseableHttpAsyncClient client;
    private final FutureCallback<SimpleHttpResponse> responseHandler;
    private final SimpleHttpRequest request;

    public ApacheHttpClient(SimpleHttpRequest request, FutureCallback<SimpleHttpResponse> responseHandler) {
        this.request = request;
        this.responseHandler = responseHandler;

        // by default HttpClient uses CachedThreadPool with no limits
        client = HttpAsyncClients.custom()
                .disableCookieManagement()
                .disableAuthCaching()
                .build();
        client.start();
    }

    @Override
    public void executeRequest() {
        client.execute(request, CONTEXT, responseHandler);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    public static class ApacheResponseHandler implements FutureCallback<SimpleHttpResponse> {
        private final CountDownLatch latch;
        private final Log logger;

        public ApacheResponseHandler(CountDownLatch latch, Log logger) {
            this.latch = latch;
            this.logger = logger;
        }

        @Override
        public void completed(SimpleHttpResponse response) {
            latch.countDown();
            if (response.getCode() != 200) {
                logger.error("Response is not successful: %s").with(response.getCode());
            }
        }

        @Override
        public void failed(Exception e) {
            latch.countDown();
            logger.error().append(e).commit();
        }

        @Override
        public void cancelled() {
            latch.countDown();
            logger.error().append("Request has been cancelled").commit();
        }
    }
}
