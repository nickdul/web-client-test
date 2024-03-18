package deltix.web.client.http;

import com.epam.deltix.gflog.api.Log;
import deltix.web.client.WebClient;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

public final class JavaHttpClient implements WebClient {
    private final HttpClient client;
    private final HttpRequest request;
    private final BiFunction<HttpResponse<Void>, Throwable, Object> responseHandler;

    public JavaHttpClient(HttpRequest request, BiFunction<HttpResponse<Void>, Throwable, Object> responseHandler) {
        this.request = request;
        this.responseHandler = responseHandler;

        // by default HttpClient uses CachedThreadPool with no limits
        final HttpClient.Builder builder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1);
        client = builder.build();
    }

    @Override
    public void executeRequest() {
        client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .toCompletableFuture().handle(responseHandler);
    }

    @Override
    public void close() {
    }

    public static class JavaResponseHandler implements BiFunction<HttpResponse<Void>, Throwable, Object> {
        private final CountDownLatch latch;
        private final Log logger;

        public JavaResponseHandler(CountDownLatch latch, Log logger) {
            this.latch = latch;
            this.logger = logger;
        }

        @Override
        public Object apply(HttpResponse response, Throwable throwable) {
            latch.countDown();

            if (throwable != null) {
                logger.error().append(throwable).commit();
                return response;
            }
            if (response.statusCode() != 200) {
                logger.error("Response is not successful: %s").with(response.statusCode());
            }
            return response;
        }
    }
}
