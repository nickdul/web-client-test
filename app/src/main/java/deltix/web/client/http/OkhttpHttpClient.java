package deltix.web.client.http;

import com.epam.deltix.gflog.api.Log;
import deltix.web.client.WebClient;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public final class OkhttpHttpClient implements WebClient {
    private final OkHttpClient client;
    private final Request request;
    private final ResponseCallback callback;

    public OkhttpHttpClient(Request request, ResponseCallback callback) throws Exception {
        this.request = request;
        this.callback = callback;

        final Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(512); // increase max concurrent requests limit (default is 64)
        dispatcher.setMaxRequestsPerHost(512); // increase max concurrent requests per host (default is 5)

        final OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .dispatcher(dispatcher);
        client = builder.build();
    }

    @Override
    public void executeRequest() {
        client.newCall(request).enqueue(callback);
    }

    @Override
    public void close() throws IOException {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
    }

    public static class ResponseCallback implements Callback {
        private final CountDownLatch latch;
        private final Log logger;

        public ResponseCallback(CountDownLatch latch, Log logger) {
            this.latch = latch;
            this.logger = logger;
        }

        @Override
        public void onFailure(@NotNull Call call, @NotNull IOException e) {
            latch.countDown();
            logger.error().append(e).commit();
        }

        @Override
        public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
            latch.countDown();
            if (response.code() != 200) {
                logger.error("Response is not successful: %s").with(response.code());
            }
            final ResponseBody body = response.body();
            if (body != null) {
                body.close();
            }
        }
    }
}
