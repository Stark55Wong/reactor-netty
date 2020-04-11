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

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.http.HttpProtocol;
import reactor.netty.tcp.SslProvider;
import reactor.netty.transport.TransportServer;
import reactor.util.Logger;
import reactor.util.Loggers;

import static reactor.netty.ReactorNetty.format;

/**
 * An HttpServer allows to build in a safe immutable way an HTTP server that is
 * materialized and connecting when {@link #bind()} is ultimately called.
 * <p>
 * <p>Examples:
 * <pre>
 * {@code
 * HttpServer.create()
 *           .host("0.0.0.0")
 *           .handle((req, res) -> res.sendString(Flux.just("hello")))
 *           .bind()
 *           .block();
 * }
 * </pre>
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 */
public abstract class HttpServer extends TransportServer<HttpServer, HttpServerConfig> {

	/**
	 * Prepare an {@link HttpServer}
	 *
	 * @return a new {@link HttpServer}
	 */
	public static HttpServer create() {
		return HttpServerBind.INSTANCE;
	}

	/**
	 * Enable GZip response compression if the client request presents accept encoding
	 * headers and the provided {@link java.util.function.Predicate} matches.
	 * <p>
	 * Note: the passed {@link HttpServerRequest} and {@link HttpServerResponse}
	 * should be considered read-only and the implement SHOULD NOT consume or
	 * write the request/response in this predicate.
	 * </p>
	 *
	 * @param predicate that returns true to compress the response.
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer compress(BiPredicate<HttpServerRequest, HttpServerResponse> predicate) {
		Objects.requireNonNull(predicate, "compressionPredicate");
		HttpServer dup = duplicate();
		dup.configuration().compressPredicate = predicate;
		return dup;
	}

	/**
	 * Specifies whether GZip response compression is enabled if the client request
	 * presents accept encoding.
	 *
	 * @param compressionEnabled if true GZip response compression
	 * is enabled if the client request presents accept encoding, otherwise disabled.
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer compress(boolean compressionEnabled) {
		HttpServer dup = duplicate();
		if (compressionEnabled) {
			dup.configuration().minCompressionSize = 0;
		}
		else {
			dup.configuration().minCompressionSize = -1;
			dup.configuration().compressPredicate = null;
		}
		return dup;
	}

	/**
	 * Enable GZip response compression if the client request presents accept encoding
	 * headers AND the response reaches a minimum threshold
	 *
	 * @param minResponseSize compression is performed once response size exceeds the given
	 * value in bytes
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer compress(int minResponseSize) {
		if (minResponseSize < 0) {
			throw new IllegalArgumentException("minResponseSize must be positive");
		}
		HttpServer dup = duplicate();
		dup.configuration().minCompressionSize = minResponseSize;
		return dup;
	}

	/**
	 * Configure the
	 * {@link ServerCookieEncoder}; {@link ServerCookieDecoder} will be
	 * chosen based on the encoder
	 *
	 * @param encoder the preferred ServerCookieEncoder
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer cookieCodec(ServerCookieEncoder encoder) {
		Objects.requireNonNull(encoder, "encoder");
		ServerCookieDecoder decoder = encoder == ServerCookieEncoder.LAX ?
				ServerCookieDecoder.LAX : ServerCookieDecoder.STRICT;
		HttpServer dup = duplicate();
		dup.configuration().cookieEncoder = encoder;
		dup.configuration().cookieDecoder = decoder;
		return dup;
	}

	/**
	 * Configure the
	 * {@link ServerCookieEncoder} and {@link ServerCookieDecoder}
	 *
	 * @param encoder the preferred ServerCookieEncoder
	 * @param decoder the preferred ServerCookieDecoder
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer cookieCodec(ServerCookieEncoder encoder, ServerCookieDecoder decoder) {
		Objects.requireNonNull(encoder, "encoder");
		Objects.requireNonNull(decoder, "decoder");
		HttpServer dup = duplicate();
		dup.configuration().cookieEncoder = encoder;
		dup.configuration().cookieDecoder = decoder;
		return dup;
	}

	/**
	 * Specifies whether support for the {@code "Forwarded"} and {@code "X-Forwarded-*"}
	 * HTTP request headers for deriving information about the connection is enabled.
	 *
	 * @param forwardedEnabled if true support for the {@code "Forwarded"} and {@code "X-Forwarded-*"}
	 *                         HTTP request headers for deriving information about the connection is enabled,
	 *                         otherwise disabled.
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer forwarded(boolean forwardedEnabled) {
		HttpServer dup = duplicate();
		dup.configuration().forwarded = forwardedEnabled;
		return dup;
	}

	/**
	 * Attach an I/O handler to react on a connected client
	 *
	 * @param handler an I/O handler that can dispose underlying connection when {@link
	 * Publisher} terminates. Only the first registered handler will subscribe to the
	 * returned {@link Publisher} while other will immediately cancel given a same
	 * {@link Connection}
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer handle(BiFunction<? super HttpServerRequest, ? super
			HttpServerResponse, ? extends Publisher<Void>> handler) {
		Objects.requireNonNull(handler, "handler");
		return childObserve(new HttpServerHandle(handler));
	}

	/**
	 * Configure the {@link io.netty.handler.codec.http.HttpServerCodec}'s request decoding options.
	 *
	 * @param requestDecoderOptions a function to mutate the provided Http request decoder options
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer httpRequestDecoder(Function<HttpRequestDecoderSpec, HttpRequestDecoderSpec> requestDecoderOptions) {
		Objects.requireNonNull(requestDecoderOptions, "requestDecoderOptions");
		HttpServer dup = duplicate();
		dup.configuration().decoder = requestDecoderOptions.apply(new HttpRequestDecoderSpec()).build();
		return dup;
	}

	/**
	 * Removes any previously applied SSL configuration customization
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer noSSL() {
		if (configuration().isSecure()) {
			HttpServer dup = duplicate();
			dup.configuration().sslProvider = null;
			return dup;
		}
		return this;
	}

	/**
	 * The HTTP protocol to support. Default is {@link HttpProtocol#HTTP11}.
	 *
	 * @param supportedProtocols The various {@link HttpProtocol} this server will support
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer protocol(HttpProtocol... supportedProtocols) {
		Objects.requireNonNull(supportedProtocols, "supportedProtocols");
		HttpServer dup = duplicate();
		dup.configuration().protocols = HttpServerConfig.protocols(supportedProtocols);
		return dup;
	}

	/**
	 * Specifies whether support for the {@code "HAProxy proxy protocol"}
	 * for deriving information about the address of the remote peer is enabled.
	 *
	 * @param proxyProtocolSupportType
	 *      <ul>
	 *         <li>
	 *             choose {@link ProxyProtocolSupportType#ON}
	 *             to enable support for the {@code "HAProxy proxy protocol"}
	 *             for deriving information about the address of the remote peer.
	 *         </li>
	 *         <li>choose {@link ProxyProtocolSupportType#OFF} to disable the proxy protocol support.</li>
	 *         <li>
	 *             choose {@link ProxyProtocolSupportType#AUTO}
	 *             then each connection of the same {@link HttpServer} will auto detect whether there is proxy protocol,
	 *             so {@link HttpServer} can accept requests with or without proxy protocol at the same time.
	 *         </li>
	 *      </ul>
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer proxyProtocol(ProxyProtocolSupportType proxyProtocolSupportType) {
		Objects.requireNonNull(proxyProtocolSupportType, "The parameter: proxyProtocolSupportType must not be null.");
		if (proxyProtocolSupportType == ProxyProtocolSupportType.ON ||
				proxyProtocolSupportType == ProxyProtocolSupportType.AUTO) {
			if (!HAProxyMessageReader.hasProxyProtocol()) {
				throw new UnsupportedOperationException(
						"To enable proxyProtocol, you must add the dependency `io.netty:netty-codec-haproxy`" +
								" to the class path first");
			}
		}
		HttpServer dup = duplicate();
		dup.configuration().proxyProtocolSupportType = proxyProtocolSupportType;
		return dup;
	}

	/**
	 * Define routes for the server through the provided {@link HttpServerRoutes} builder.
	 *
	 * @param routesBuilder provides a route builder to be mutated in order to define routes.
	 * @return a new {@link HttpServer} starting the router on subscribe
	 */
	public final HttpServer route(Consumer<? super HttpServerRoutes> routesBuilder) {
		Objects.requireNonNull(routesBuilder, "routeBuilder");
		HttpServerRoutes routes = HttpServerRoutes.newRoutes();
		routesBuilder.accept(routes);
		return handle(routes);
	}

	/**
	 * Apply an SSL configuration customization via the passed builder. The builder
	 * will produce the {@link SslContext} to be passed to with a default value of
	 * {@code 10} seconds handshake timeout unless the environment property {@code
	 * reactor.netty.tcp.sslHandshakeTimeout} is set.
	 *
	 * If {@link SelfSignedCertificate} needs to be used, the sample below can be
	 * used. Note that {@link SelfSignedCertificate} should not be used in production.
	 * <pre>
	 * {@code
	 *     SelfSignedCertificate cert = new SelfSignedCertificate();
	 *     SslContextBuilder sslContextBuilder =
	 *             SslContextBuilder.forServer(cert.certificate(), cert.privateKey());
	 *     secure(sslContextSpec -> sslContextSpec.sslContext(sslContextBuilder));
	 * }
	 * </pre>
	 *
	 * @param sslProviderBuilder builder callback for further customization of SslContext.
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer secure(Consumer<? super SslProvider.SslContextSpec> sslProviderBuilder) {
		Objects.requireNonNull(sslProviderBuilder, "sslProviderBuilder");
		HttpServer dup = duplicate();
		SslProvider.SslContextSpec builder = SslProvider.builder();
		sslProviderBuilder.accept(builder);
		dup.configuration().sslProvider = ((SslProvider.Builder) builder).build();
		return dup;
	}

	/**
	 * Applies an SSL configuration via the passed {@link SslProvider}.
	 *
	 * If {@link SelfSignedCertificate} needs to be used, the sample below can be
	 * used. Note that {@link SelfSignedCertificate} should not be used in production.
	 * <pre>
	 * {@code
	 *     SelfSignedCertificate cert = new SelfSignedCertificate();
	 *     SslContextBuilder sslContextBuilder =
	 *             SslContextBuilder.forServer(cert.certificate(), cert.privateKey());
	 *     secure(sslContextSpec -> sslContextSpec.sslContext(sslContextBuilder));
	 * }
	 * </pre>
	 *
	 * @param sslProvider The provider to set when configuring SSL
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer secure(SslProvider sslProvider) {
		Objects.requireNonNull(sslProvider, "sslProvider");
		HttpServer dup = duplicate();
		dup.configuration().sslProvider = sslProvider;
		return dup;
	}

	static final Logger log = Loggers.getLogger(HttpServer.class);

	static final class HttpServerHandle implements ConnectionObserver {

		final BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler;

		HttpServerHandle(BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler) {
			this.handler = handler;
		}

		@Override
		@SuppressWarnings("FutureReturnValueIgnored")
		public void onStateChange(Connection connection, State newState) {
			if (newState == HttpServerState.REQUEST_RECEIVED) {
				try {
					if (log.isDebugEnabled()) {
						log.debug(format(connection.channel(), "Handler is being applied: {}"), handler);
					}
					HttpServerOperations ops = (HttpServerOperations) connection;
					Mono.fromDirect(handler.apply(ops, ops))
					    .subscribe(ops.disposeSubscriber());
				}
				catch (Throwable t) {
					log.error(format(connection.channel(), ""), t);
					//"FutureReturnValueIgnored" this is deliberate
					connection.channel()
					          .close();
				}
			}
		}
	}
}
