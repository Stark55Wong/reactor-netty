/*
 * Copyright (c) 2011-Present VMware, Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.http.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ReflectiveChannelFactory;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.codec.http2.CleartextHttp2ServerUpgradeHandler;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.util.AsciiString;
import reactor.netty.ChannelPipelineConfigurer;
import reactor.netty.ConnectionObserver;
import reactor.netty.NettyPipeline;
import reactor.netty.ReactorNetty;
import reactor.netty.channel.ChannelMetricsRecorder;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.HttpResources;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpServer;
import reactor.netty.transport.TransportServerConfig;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import static reactor.netty.ReactorNetty.ACCESS_LOG_ENABLED;
import static reactor.netty.ReactorNetty.format;

/**
 * Encapsulate all necessary configuration for HTTP server transport.
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 */
public final class HttpServerConfig extends TransportServerConfig<HttpServerConfig> {

	BiPredicate<HttpServerRequest, HttpServerResponse> compressPredicate;
	ServerCookieDecoder cookieDecoder;
	ServerCookieEncoder cookieEncoder;
	HttpRequestDecoderSpec decoder;
	boolean forwarded;
	int minCompressionSize;
	int protocols;
	ProxyProtocolSupportType proxyProtocolSupportType;
	SslProvider sslProvider;

	HttpServerConfig(Map<ChannelOption<?>, ?> options, Map<ChannelOption<?>, ?> childOptions, Supplier<? extends SocketAddress> localAddress) {
		super(options, childOptions, localAddress);
		this.cookieDecoder = ServerCookieDecoder.STRICT;
		this.cookieEncoder = ServerCookieEncoder.STRICT;
		this.decoder = new HttpRequestDecoderSpec();
		this.forwarded = false;
		this.minCompressionSize = -1;
		this.protocols = h11;
		this.proxyProtocolSupportType = ProxyProtocolSupportType.OFF;
	}

	HttpServerConfig(HttpServerConfig parent) {
		super(parent);
		this.compressPredicate = parent.compressPredicate;
		this.cookieDecoder = parent.cookieDecoder;
		this.cookieEncoder = parent.cookieEncoder;
		this.decoder = parent.decoder;
		this.forwarded = parent.forwarded;
		this.minCompressionSize = parent.minCompressionSize;
		this.protocols = parent.protocols;
		this.proxyProtocolSupportType = parent.proxyProtocolSupportType;
		this.sslProvider = parent.sslProvider;
	}

	/**
	 * Return the configured compression predicate or null.
	 *
	 * @return the configured compression predicate or null
	 */
	@Nullable
	public BiPredicate<HttpServerRequest, HttpServerResponse> compressPredicate() {
		return compressPredicate;
	}

	/**
	 * Return the configured {@link ServerCookieDecoder} or the default {@link ServerCookieDecoder#STRICT}.
	 *
	 * @return the configured {@link ServerCookieDecoder} or the default {@link ServerCookieDecoder#STRICT}
	 */
	public ServerCookieDecoder cookieDecoder() {
		return cookieDecoder;
	}

	/**
	 * Return the configured {@link ServerCookieEncoder} or the default {@link ServerCookieEncoder#STRICT}.
	 *
	 * @return the configured {@link ServerCookieEncoder} or the default {@link ServerCookieEncoder#STRICT}
	 */
	public ServerCookieEncoder cookieEncoder() {
		return cookieEncoder;
	}

	/**
	 * Return the configured HTTP request decoder options or the default.
	 *
	 * @return the configured HTTP request decoder options or the default
	 */
	public HttpRequestDecoderSpec decoder() {
		return decoder;
	}

	/**
	 * Returns whether that {@link HttpServer} supports the {@code "Forwarded"} and {@code "X-Forwarded-*"}
	 * HTTP request headers for deriving information about the connection.
	 *
	 * @return true if that {@link HttpServer} supports the {@code "Forwarded"} and {@code "X-Forwarded-*"}
	 * HTTP request headers for deriving information about the connection
	 */
	public boolean isForwarded() {
		return forwarded;
	}

	/**
	 * Returns true if that {@link HttpServer} secured via SSL transport
	 *
	 * @return true if that {@link HttpServer} secured via SSL transport
	 */
	public final boolean isSecure(){
		return sslProvider != null;
	}

	public int minCompressionSize() {
		return minCompressionSize;
	}

	public int protocols() {
		return protocols;
	}

	public ProxyProtocolSupportType proxyProtocolSupportType() {
		return proxyProtocolSupportType;
	}

	/**
	 * Returns the current {@link SslProvider} if that {@link TcpServer} secured via SSL
	 * transport or null
	 *
	 * @return the current {@link SslProvider} if that {@link TcpServer} secured via SSL
	 * transport or null
	 */
	@Nullable
	public SslProvider sslProvider() {
		return sslProvider;
	}

	@Override
	protected EventLoopGroup childEventLoopGroup() {
		return loopResources().onServer(LoopResources.DEFAULT_NATIVE);
	}

	@Override
	protected ChannelFactory<? extends Channel> connectionFactory(EventLoopGroup elg) {
		return new ReflectiveChannelFactory<>(loopResources().onServerChannel(elg));
	}

	@Override
	protected LoggingHandler defaultLoggingHandler() {
		return LOGGING_HANDLER;
	}

	@Override
	protected LoopResources defaultLoopResources() {
		return HttpResources.get();
	}

	@Override
	protected ChannelMetricsRecorder defaultMetricsRecorder() {
		return MicrometerHttpServerMetricsRecorder.INSTANCE;
	}

	@Override
	protected EventLoopGroup eventLoopGroup() {
		return loopResources().onServerSelect(LoopResources.DEFAULT_NATIVE);
	}

	@Override
	protected ChannelPipelineConfigurer defaultOnChannelInit() {
		return (channel, observer, remoteAddress) -> {
			if (sslProvider != null) {
				sslProvider.addSslHandler(channel, remoteAddress, SSL_DEBUG);

				if ((protocols & h11orH2) == h11orH2) {
					channel.pipeline()
					       .addLast(new Http11OrH2Codec(
					               decoder.initialBufferSize(),
					               decoder.maxChunkSize(),
					               compressPredicate(compressPredicate, minCompressionSize),
					               cookieDecoder,
					               cookieEncoder,
					               forwarded,
					               decoder.maxHeaderSize(),
					               decoder.maxInitialLineLength(),
					               observer,
							       metricsRecorder(),
					               minCompressionSize,
					               channelOperationsProvider(),
					               decoder.validateHeaders()));
				}
				if ((protocols & h11) == h11) {
					configureHttp11Pipeline(channel.pipeline(),
							decoder.initialBufferSize(),
							decoder.maxChunkSize(),
							compressPredicate(compressPredicate, minCompressionSize),
							cookieDecoder,
							cookieEncoder,
							forwarded,
							decoder.maxHeaderSize(),
							decoder.maxInitialLineLength(),
							observer,
							metricsRecorder(),
							minCompressionSize,
							decoder.validateHeaders());
				}
				if ((protocols & h2) == h2) {
					configureH2Pipeline(channel.pipeline(),
							cookieDecoder,
							cookieEncoder,
							forwarded,
							observer,
							channelOperationsProvider(),
							decoder.validateHeaders());
				}
			}
			else {
				if ((protocols & h11orH2c) == h11orH2c) {
					configureHttp11OrH2CleartextPipeline(channel.pipeline(),
							decoder.initialBufferSize(),
							decoder.maxChunkSize(),
							compressPredicate(compressPredicate, minCompressionSize),
							cookieDecoder,
							cookieEncoder,
							forwarded,
							decoder.maxHeaderSize(),
							decoder.maxInitialLineLength(),
							observer,
							metricsRecorder(),
							minCompressionSize,
							channelOperationsProvider(),
							decoder.validateHeaders());
				}
				if ((protocols & h11) == h11) {
					configureHttp11Pipeline(channel.pipeline(),
							decoder.initialBufferSize(),
							decoder.maxChunkSize(),
							compressPredicate(compressPredicate, minCompressionSize),
							cookieDecoder,
							cookieEncoder,
							forwarded,
							decoder.maxHeaderSize(),
							decoder.maxInitialLineLength(),
							observer,
							metricsRecorder(),
							minCompressionSize,
							decoder.validateHeaders());
				}
				if ((protocols & h2c) == h2c) {
					configureH2Pipeline(channel.pipeline(),
							cookieDecoder,
							cookieEncoder,
							forwarded,
							observer,
							channelOperationsProvider(),
							decoder.validateHeaders());
				}
			}

			if (proxyProtocolSupportType == ProxyProtocolSupportType.ON ||
					proxyProtocolSupportType == ProxyProtocolSupportType.AUTO) {
				if (proxyProtocolSupportType == ProxyProtocolSupportType.ON) {
					channel.pipeline()
					       .addFirst(NettyPipeline.ProxyProtocolDecoder, new HAProxyMessageDecoder())
					       .addAfter(NettyPipeline.ProxyProtocolDecoder,
					                 NettyPipeline.ProxyProtocolReader, new HAProxyMessageReader());
				}
				else { // AUTO
					channel.pipeline()
					       .addFirst(NettyPipeline.ProxyProtocolDecoder, new HAProxyMessageDetector());
				}
			}
		};
	}

	static void addStreamHandlers(Channel ch, ChannelOperations.OnSetup opsFactory,
			ConnectionObserver listener, boolean readForwardHeaders,
			ServerCookieEncoder encoder, ServerCookieDecoder decoder) {
		if (ACCESS_LOG) {
			ch.pipeline()
			  .addLast(NettyPipeline.AccessLogHandler, new AccessLogHandlerH2());
		}
		ch.pipeline()
		  .addLast(new Http2StreamFrameToHttpObjectCodec(true))
		  .addLast(new Http2StreamBridgeHandler(listener, readForwardHeaders, encoder, decoder));

		ChannelOperations.addReactiveBridge(ch, opsFactory, listener);

		if (log.isDebugEnabled()) {
			log.debug(format(ch, "Initialized HTTP/2 pipeline {}"), ch.pipeline());
		}
	}

	@Nullable
	static BiPredicate<HttpServerRequest, HttpServerResponse> compressPredicate(
			@Nullable BiPredicate<HttpServerRequest, HttpServerResponse> compressionPredicate,
			int minResponseSize) {

		if (minResponseSize <= 0) {
			return compressionPredicate;
		}

		BiPredicate<HttpServerRequest, HttpServerResponse> lengthPredicate =
				(req, res) -> {
					String length = res.responseHeaders()
					                   .get(HttpHeaderNames.CONTENT_LENGTH);

					if (length == null) {
						return true;
					}

					try {
						return Long.parseLong(length) >= minResponseSize;
					}
					catch (NumberFormatException nfe) {
						return true;
					}
				};

		if (compressionPredicate != null) {
			lengthPredicate = lengthPredicate.and(compressionPredicate);
		}
		return lengthPredicate;
	}

	static void configureH2Pipeline(ChannelPipeline p,
			ServerCookieDecoder cookieDecoder,
			ServerCookieEncoder cookieEncoder,
			boolean forwarded,
			ConnectionObserver listener,
			ChannelOperations.OnSetup opsFactory,
			boolean validate) {
		p.remove(NettyPipeline.ReactiveBridge);

		Http2FrameCodecBuilder http2FrameCodecBuilder =
				Http2FrameCodecBuilder.forServer()
				                      .validateHeaders(validate)
				                      .initialSettings(Http2Settings.defaultSettings());

		if (p.get(NettyPipeline.LoggingHandler) != null) {
			http2FrameCodecBuilder.frameLogger(new Http2FrameLogger(LogLevel.DEBUG,
					"reactor.netty.http.server.h2"));
		}

		p.addLast(NettyPipeline.HttpCodec, http2FrameCodecBuilder.build())
		 .addLast(new Http2MultiplexHandler(new H2Codec(opsFactory, listener, forwarded, cookieEncoder, cookieDecoder)));
	}

	static void configureHttp11OrH2CleartextPipeline(ChannelPipeline p,
			int buffer,
			int chunk,
			@Nullable BiPredicate<HttpServerRequest, HttpServerResponse> compressPredicate,
			ServerCookieDecoder cookieDecoder,
			ServerCookieEncoder cookieEncoder,
			boolean forwarded,
			int header,
			int line,
			ConnectionObserver listener,
			@Nullable Supplier<? extends ChannelMetricsRecorder> metricsRecorder,
			int minCompressionSize,
			ChannelOperations.OnSetup opsFactory,
			boolean validate) {
		HttpServerCodec httpServerCodec =
				new HttpServerCodec(line, header, chunk, validate, buffer);

		Http11OrH2CleartextCodec
				upgrader = new Http11OrH2CleartextCodec(cookieDecoder, cookieEncoder, p.get(NettyPipeline.LoggingHandler) != null,
						forwarded, listener, opsFactory, validate);

		ChannelHandler http2ServerHandler = new H2CleartextCodec(upgrader);
		CleartextHttp2ServerUpgradeHandler h2cUpgradeHandler = new CleartextHttp2ServerUpgradeHandler(
				httpServerCodec,
				new HttpServerUpgradeHandler(httpServerCodec, upgrader),
				http2ServerHandler);

		p.addLast(h2cUpgradeHandler);

		if (ACCESS_LOG) {
			p.addLast(NettyPipeline.AccessLogHandler, new AccessLogHandler());
		}

		boolean alwaysCompress = compressPredicate == null && minCompressionSize == 0;

		if (alwaysCompress) {
			p.addLast(NettyPipeline.CompressionHandler, new SimpleCompressionHandler());
		}

		p.addLast(NettyPipeline.HttpTrafficHandler,
				new HttpTrafficHandler(listener, forwarded, compressPredicate, cookieEncoder, cookieDecoder));

		if (metricsRecorder != null) {
			ChannelMetricsRecorder channelMetricsRecorder = metricsRecorder.get();
			if (channelMetricsRecorder instanceof HttpServerMetricsRecorder) {
				p.addAfter(NettyPipeline.HttpTrafficHandler, NettyPipeline.HttpMetricsHandler,
						new HttpServerMetricsHandler((HttpServerMetricsRecorder) channelMetricsRecorder));
			}
		}
	}

	static void configureHttp11Pipeline(ChannelPipeline p,
			int buffer,
			int chunk,
			@Nullable BiPredicate<HttpServerRequest, HttpServerResponse> compressPredicate,
			ServerCookieDecoder cookieDecoder,
			ServerCookieEncoder cookieEncoder,
			boolean forwarded,
			int header,
			int line,
			ConnectionObserver listener,
			@Nullable Supplier<? extends ChannelMetricsRecorder> metricsRecorder,
			int minCompressionSize,
			boolean validate) {
		p.addBefore(NettyPipeline.ReactiveBridge,
		            NettyPipeline.HttpCodec,
		            new HttpServerCodec(line, header, chunk, validate, buffer))
		 .addBefore(NettyPipeline.ReactiveBridge,
		            NettyPipeline.HttpTrafficHandler,
		            new HttpTrafficHandler(listener, forwarded, compressPredicate, cookieEncoder, cookieDecoder));

		if (ACCESS_LOG) {
			p.addAfter(NettyPipeline.HttpCodec, NettyPipeline.AccessLogHandler, new AccessLogHandler());
		}

		boolean alwaysCompress = compressPredicate == null && minCompressionSize == 0;

		if (alwaysCompress) {
			p.addBefore(NettyPipeline.HttpTrafficHandler, NettyPipeline.CompressionHandler, new SimpleCompressionHandler());
		}

		if (metricsRecorder != null) {
			ChannelMetricsRecorder channelMetricsRecorder = metricsRecorder.get();
			if (channelMetricsRecorder instanceof HttpServerMetricsRecorder) {
				p.addAfter(NettyPipeline.HttpTrafficHandler, NettyPipeline.HttpMetricsHandler,
				           new HttpServerMetricsHandler((HttpServerMetricsRecorder) channelMetricsRecorder));
			}
		}
	}

	static int protocols(HttpProtocol... protocols) {
		int _protocols = 0;

		for (HttpProtocol p : protocols) {
			if (p == HttpProtocol.HTTP11) {
				_protocols |= h11;
			}
			else if (p == HttpProtocol.H2) {
				_protocols |= h2;
			}
			else if (p == HttpProtocol.H2C) {
				_protocols |= h2c;
			}
		}
		return _protocols;
	}

	static final boolean ACCESS_LOG = Boolean.parseBoolean(System.getProperty(ACCESS_LOG_ENABLED, "false"));

	static final Logger log = Loggers.getLogger(HttpServerConfig.class);

	static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(HttpServer.class);

	static final int h11 = 0b100;

	static final int h2 = 0b010;

	static final int h2c = 0b001;

	static final int h11orH2c = h11 | h2c;

	static final int h11orH2 = h11 | h2;

	/**
	 * Default value whether the SSL debugging on the server side will be enabled/disabled,
	 * fallback to SSL debugging disabled
	 */
	static final boolean SSL_DEBUG = Boolean.parseBoolean(System.getProperty(ReactorNetty.SSL_SERVER_DEBUG, "false"));

	static final class H2CleartextCodec extends ChannelHandlerAdapter {

		final Http11OrH2CleartextCodec upgrader;

		H2CleartextCodec(Http11OrH2CleartextCodec upgrader) {
			this.upgrader = upgrader;
		}

		@Override
		public void handlerAdded(ChannelHandlerContext ctx) {
			ChannelPipeline pipeline = ctx.pipeline();
			pipeline.addAfter(ctx.name(), NettyPipeline.HttpCodec, upgrader.http2FrameCodec)
					.addAfter(NettyPipeline.HttpCodec, null, new Http2MultiplexHandler(upgrader))
					.remove(this);
			if (pipeline.get(NettyPipeline.AccessLogHandler) != null){
				pipeline.remove(NettyPipeline.AccessLogHandler);
			}
			if (pipeline.get(NettyPipeline.CompressionHandler) != null) {
				pipeline.remove(NettyPipeline.CompressionHandler);
			}
			pipeline.remove(NettyPipeline.HttpTrafficHandler);
			pipeline.remove(NettyPipeline.ReactiveBridge);
		}
	}

	static final class H2Codec extends ChannelInitializer<Channel> {

		final boolean                   forwarded;
		final ConnectionObserver        listener;
		final ServerCookieEncoder       cookieEncoder;
		final ServerCookieDecoder       cookieDecoder;
		final ChannelOperations.OnSetup opsFactory;

		H2Codec(ChannelOperations.OnSetup opsFactory,ConnectionObserver listener, boolean forwarded,
		ServerCookieEncoder encoder, ServerCookieDecoder decoder) {
			this.forwarded = forwarded;
			this.listener = listener;
			this.cookieEncoder = encoder;
			this.cookieDecoder = decoder;
			this.opsFactory = opsFactory;
		}

		@Override
		protected void initChannel(Channel ch) {
			addStreamHandlers(ch, opsFactory, listener, forwarded, cookieEncoder, cookieDecoder);
		}
	}

	/**
	 * Initialize Http11 - H2 pipeline configuration using packet inspection or cleartext upgrade
	 */
	@ChannelHandler.Sharable
	static final class Http11OrH2CleartextCodec extends ChannelInitializer<Channel>
			implements HttpServerUpgradeHandler.UpgradeCodecFactory {

		final ServerCookieDecoder       cookieDecoder;
		final ServerCookieEncoder       cookieEncoder;
		final boolean                   forwarded;
		final Http2FrameCodec           http2FrameCodec;
		final ConnectionObserver        listener;
		final ChannelOperations.OnSetup opsFactory;

		Http11OrH2CleartextCodec(
				ServerCookieDecoder cookieDecoder,
				ServerCookieEncoder cookieEncoder,
				boolean debug,
				boolean forwarded,
				ConnectionObserver listener,
				ChannelOperations.OnSetup opsFactory,
				boolean validate) {
			this.cookieDecoder = cookieDecoder;
			this.cookieEncoder = cookieEncoder;
			this.forwarded = forwarded;
			Http2FrameCodecBuilder http2FrameCodecBuilder =
					Http2FrameCodecBuilder.forServer()
					                      .validateHeaders(validate)
					                      .initialSettings(Http2Settings.defaultSettings());

			if (debug) {
				http2FrameCodecBuilder.frameLogger(new Http2FrameLogger(
						LogLevel.DEBUG,
						"reactor.netty.http.server.h2"));
			}
			this.http2FrameCodec = http2FrameCodecBuilder.build();
			this.listener = listener;
			this.opsFactory = opsFactory;
		}

		/**
		 * Inline channel initializer
		 */
		@Override
		protected void initChannel(Channel ch) {
			addStreamHandlers(ch, opsFactory, listener, forwarded, cookieEncoder, cookieDecoder);
		}

		@Override
		@Nullable
		public HttpServerUpgradeHandler.UpgradeCodec newUpgradeCodec(CharSequence protocol) {
			if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME, protocol)) {
				return new Http2ServerUpgradeCodec(http2FrameCodec, new Http2MultiplexHandler(this));
			}
			else {
				return null;
			}
		}
	}

	static final class Http11OrH2Codec extends ApplicationProtocolNegotiationHandler {

		final int                                                buffer;
		final int                                                chunk;
		final BiPredicate<HttpServerRequest, HttpServerResponse> compressPredicate;
		final ServerCookieDecoder                                cookieDecoder;
		final ServerCookieEncoder                                cookieEncoder;
		final boolean                                            forwarded;
		final int                                                header;
		final int                                                line;
		final ConnectionObserver                                 listener;
		final Supplier<? extends ChannelMetricsRecorder>         metricsRecorder;
		final int                                                minCompressionSize;
		final ChannelOperations.OnSetup                          opsFactory;
		final boolean                                            validate;

		Http11OrH2Codec(
				int buffer,
				int chunk,
				@Nullable BiPredicate<HttpServerRequest, HttpServerResponse> compressPredicate,
				ServerCookieDecoder decoder,
				ServerCookieEncoder encoder,
				boolean forwarded,
				int header,
				int line,
				ConnectionObserver listener,
				@Nullable Supplier<? extends ChannelMetricsRecorder> metricsRecorder,
				int minCompressionSize,
				ChannelOperations.OnSetup opsFactory,
				boolean validate) {
			super(ApplicationProtocolNames.HTTP_1_1);
			this.buffer = buffer;
			this.chunk = chunk;
			this.compressPredicate = compressPredicate;
			this.cookieDecoder = decoder;
			this.cookieEncoder = encoder;
			this.forwarded = forwarded;
			this.header = header;
			this.line = line;
			this.listener = listener;
			this.metricsRecorder = metricsRecorder;
			this.minCompressionSize = minCompressionSize;
			this.opsFactory = opsFactory;
			this.validate = validate;
		}

		@Override
		protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
			ChannelPipeline p = ctx.pipeline();

			if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
				configureH2Pipeline(p, cookieDecoder, cookieEncoder,forwarded, listener, opsFactory, validate);
				return;
			}

			if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
				configureHttp11Pipeline(p, buffer, chunk, compressPredicate, cookieDecoder, cookieEncoder, forwarded,
						header, line, listener, metricsRecorder, minCompressionSize, validate);
				return;
			}

			throw new IllegalStateException("unknown protocol: " + protocol);
		}
	}
}
