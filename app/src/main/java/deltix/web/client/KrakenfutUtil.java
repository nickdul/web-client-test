package deltix.web.client;

import com.epam.deltix.gflog.api.Log;
import deltix.web.client.http.*;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import okhttp3.Request;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

public final class KrakenfutUtil {

    public static final String BALANCE_URL = "https://demo-futures.kraken.com/derivatives/api/v3/accounts";
    public static final byte[] BALANCE_PRESIGN = "/api/v3/accounts".getBytes(StandardCharsets.UTF_8);

    public static JavaHttpClient createJavaClient(int ioThreadCount,
                                                  String apiKey, String apiSecret,
                                                  CountDownLatch latch, Log logger) throws Exception {
        final String signature = calculateSignature(apiSecret, BALANCE_PRESIGN);

        final HttpRequest request = HttpRequest.newBuilder().GET()
                .uri(URI.create(BALANCE_URL))
                .setHeader("APIKey", apiKey)
                .setHeader("Authent", signature)
                .build();

        final JavaHttpClient.JavaResponseHandler responseHandler = new JavaHttpClient.JavaResponseHandler(latch, logger);

        return new JavaHttpClient(request, responseHandler);
    }

    public static OkhttpHttpClient createOkhttpClient(int ioThreadCount,
                                                      String apiKey, String apiSecret,
                                                      CountDownLatch latch, Log logger) throws Exception {
        final String signature = calculateSignature(apiSecret, BALANCE_PRESIGN);

        final Request request = new Request.Builder()
                .url(BALANCE_URL)
                .method("GET", null)
                .header("APIKey", apiKey)
                .header("Authent", signature)
                .build();

        final OkhttpHttpClient.ResponseCallback callback = new OkhttpHttpClient.ResponseCallback(latch, logger);

        return new OkhttpHttpClient(request, callback);
    }

    public static AhcHttpClient createAhcClient(int ioThreadCount, boolean useNativeTransport,
                                                String apiKey, String apiSecret,
                                                CountDownLatch latch, Log logger) throws Exception {
        final String signature = calculateSignature(apiSecret, BALANCE_PRESIGN);

        final RequestBuilder request = new RequestBuilder("GET")
                .setUrl(BALANCE_URL)
                .setHeader("APIKey", apiKey)
                .setHeader("Authent", signature);

        final BiFunction<Response, Throwable, Response> responseHandler =
                new AhcHttpClient.AhcResponseHandler(latch, logger);

        return new AhcHttpClient(ioThreadCount, useNativeTransport, request, responseHandler);
    }

    public static NettyHttpClient createNettyClient(int ioThreadCount, boolean useNativeTransport,
                                                    String apiKey, String apiSecret,
                                                    CountDownLatch latch, Log logger) throws Exception {
        final URI uri = new URI(BALANCE_URL);
        final String host = uri.getHost();

        final String signature = calculateSignature(apiSecret, BALANCE_PRESIGN);

        final DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath(), Unpooled.EMPTY_BUFFER);
        request.setUri(BALANCE_URL)
                .headers()
                .set("APIKey", apiKey)
                .set("Authent", signature)
                .set(HttpHeaderNames.HOST, host)
                .set(HttpHeaderNames.USER_AGENT, "AHC/2.1")
                .set(HttpHeaderNames.ACCEPT, "*/*");

        final NettyHttpClient.HttpChannelInboundHandler responseHandler =
                new NettyHttpClient.HttpChannelInboundHandler(latch, logger);

        return new NettyHttpClient(ioThreadCount, useNativeTransport, request, responseHandler);
    }


    public static ApacheHttpClient createApacheClient(String apiKey, String apiSecret,
                                                      CountDownLatch latch, Log logger) throws Exception {
        final String signature = calculateSignature(apiSecret, BALANCE_PRESIGN);

        final SimpleHttpRequest request = SimpleRequestBuilder.create("GET")
                .setUri(BALANCE_URL)
                .addHeader("APIKey", apiKey)
                .addHeader("Authent", signature)
                .build();

        final FutureCallback<SimpleHttpResponse> responseHandler =
                new ApacheHttpClient.ApacheResponseHandler(latch, logger);

        return new ApacheHttpClient(request, responseHandler);
    }

    private static String calculateSignature(String apiSecret, byte[] bytes) throws Exception {
        final Mac mac = createKrakenfutMac(apiSecret, "HmacSHA512");
        final MessageDigest messageDigest = createMessageDigest();
        messageDigest.update(bytes);
        return calculateBase64Signature(mac, messageDigest.digest());
    }

    private static MessageDigest createMessageDigest() throws NoSuchAlgorithmException {
        return MessageDigest.getInstance("SHA-256");
    }

    private static Mac createKrakenfutMac(String apiSecret, String algorithm) throws Exception {
        return createMac(Base64.getDecoder().decode(apiSecret), algorithm);
    }

    private static String calculateBase64Signature(Mac mac, byte[] bytes) {
        mac.update(bytes);
        byte[] result = mac.doFinal(); // Allocation here and we can't avoid for now
        return Base64.getEncoder().encodeToString(result);
    }

    private static Mac createMac(byte[] secret, String algorithm) throws InvalidKeyException, NoSuchAlgorithmException {
        SecretKeySpec secretKeySpec = new SecretKeySpec(secret, algorithm);
        Mac mac = Mac.getInstance(secretKeySpec.getAlgorithm());
        mac.init(secretKeySpec);
        return mac;
    }

}
