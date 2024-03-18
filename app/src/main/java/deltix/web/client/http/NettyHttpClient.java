package deltix.web.client.http;

import com.epam.deltix.gflog.api.Log;
import deltix.web.client.WebClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;


public final class NettyHttpClient implements WebClient {

    private final Channel channel;
    private final EventLoopGroup group;
    private final HttpRequest request;

    public NettyHttpClient(int ioThreadCount, boolean useNativeTransport,
                           HttpRequest request,
                           HttpChannelInboundHandler responseHandler) throws Exception {
        this.request = request;

        final SslContext sslContext = SslContextBuilder.forClient()
           .trustManager(InsecureTrustManagerFactory.INSTANCE).build();

        final String url = request.uri();
        final int start = url.indexOf("//") + 2;
        final int end = url.indexOf('/', start);
        final String host = url.substring(start, end);
        final int port = 443;

        group = useNativeTransport ? new EpollEventLoopGroup(ioThreadCount) : new NioEventLoopGroup(ioThreadCount);
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(sslContext.newHandler(ch.alloc(), host, port));
                        p.addLast(new HttpClientCodec());
                        p.addLast(new HttpObjectAggregator(1024 * 1024));
                        p.addLast(responseHandler);
                    }
                });

        channel = b.connect(host, port).sync().channel();
    }

    @Override
    public void executeRequest() {
        channel.writeAndFlush(request);
    }

    @Override
    public void close() throws IOException {
        channel.close();
        group.shutdownGracefully();
    }

    public static class HttpChannelInboundHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
        private final CountDownLatch latch;
        private final Log logger;

        public HttpChannelInboundHandler(CountDownLatch latch, Log logger) {
            this.latch = latch;
            this.logger = logger;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
            latch.countDown();
            if (msg.status().code() != 200) {
                logger.error("Response is not successful: %s").with(msg.status().code());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            latch.countDown();
            logger.error().append(cause).commit();
            ctx.close();
        }
    }
}
