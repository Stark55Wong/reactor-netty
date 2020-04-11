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

package reactor.netty.http.client;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.ByteBufMono;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;
import reactor.netty.channel.ChannelOperations;

/**
 * @author Stephane Maldini
 * @author Violeta Georgieva
 */
final class HttpClientFinalizer implements HttpClient.RequestSender {

	final HttpClientConnect httpClient;

	HttpClientFinalizer(HttpClientConnect httpClient) {
		this.httpClient = httpClient;
	}

	// UriConfiguration methods

	@Override
	public HttpClient.RequestSender uri(Mono<String> uri) {
		Objects.requireNonNull(uri, "uri");
		HttpClient dup = httpClient.duplicate();
		dup.configuration().deferredConf(client -> uri.map(s -> {
			client.config.uri = s;
			return client;
		}));
		return new HttpClientFinalizer((HttpClientConnect) dup);
	}

	@Override
	public HttpClient.RequestSender uri(String uri) {
		Objects.requireNonNull(uri, "uri");
		HttpClient dup = httpClient.duplicate();
		dup.configuration().uri = uri;
		return new HttpClientFinalizer((HttpClientConnect) dup);
	}

	// ResponseReceiver methods

	@Override
	public Mono<HttpClientResponse> response() {
		return connect().map(RESPONSE_ONLY);
	}

	@Override
	public <V> Flux<V> response(BiFunction<? super HttpClientResponse, ? super ByteBufFlux, ? extends Publisher<V>> receiver) {
		return connect().flatMapMany(resp -> Flux.from(receiver.apply(resp, resp.receive()))
		                                         .doFinally(s -> discard(resp)));
	}

	@Override
	public <V> Flux<V> responseConnection(BiFunction<? super HttpClientResponse, ? super Connection, ? extends Publisher<V>> receiver) {
		return connect().flatMapMany(resp -> Flux.from(receiver.apply(resp, resp)));
	}

	@Override
	public ByteBufFlux responseContent() {
		return content(httpClient);
	}

	@Override
	public <V> Mono<V> responseSingle(BiFunction<? super HttpClientResponse, ? super ByteBufMono, ? extends Mono<V>> receiver) {
		return connect().flatMap(resp -> receiver.apply(resp, resp.receive().aggregate())
		                                         .doFinally(s -> discard(resp)));
	}

	// RequestSender methods

	@Override
	public HttpClientFinalizer send(
			BiFunction<? super HttpClientRequest, ? super NettyOutbound, ? extends Publisher<Void>> sender) {
		Objects.requireNonNull(sender, "requestBody");
		HttpClient dup = httpClient.duplicate();
		dup.configuration().body = sender;
		return new HttpClientFinalizer((HttpClientConnect) dup);
	}

	@Override
	public HttpClientFinalizer send(Publisher<? extends ByteBuf> requestBody) {
		Objects.requireNonNull(requestBody, "requestBody");
		return send((req, out) -> out.send(requestBody));
	}

	@Override
	public HttpClientFinalizer sendForm(BiConsumer<? super HttpClientRequest, HttpClientForm> formCallback, @Nullable Consumer<Flux<Long>> progress) {
		Objects.requireNonNull(formCallback, "formCallback");
		return send((req, out) -> {
			HttpClientOperations ops = (HttpClientOperations) out;
			return new HttpClientOperations.SendForm(ops, formCallback, progress);
		});
	}

	@SuppressWarnings("unchecked")
	Mono<HttpClientOperations> connect() {
		return (Mono<HttpClientOperations>) httpClient.connect();
	}

	static ByteBufFlux content(HttpClient httpClient) {
		ByteBufAllocator alloc = (ByteBufAllocator) httpClient.configuration()
		                                                      .options()
		                                                      .get(ChannelOption.ALLOCATOR);
		if (alloc == null) {
			alloc = ByteBufAllocator.DEFAULT;
		}

		@SuppressWarnings("unchecked")
		Mono<ChannelOperations<?, ?>> connector = (Mono<ChannelOperations<?, ?>>) httpClient.connect();

		return ByteBufFlux.fromInbound(connector.flatMapMany(contentReceiver), alloc);
	}

	static void discard(HttpClientOperations c) {
		if (!c.isInboundDisposed()) {
			c.discard();
		}
	}

	static final Function<ChannelOperations<?, ?>, Publisher<ByteBuf>> contentReceiver = ChannelOperations::receive;

	static final Function<HttpClientOperations, HttpClientResponse> RESPONSE_ONLY = ops -> {
		//defer the dispose to avoid over disposing on receive
		discard(ops);
		return ops;
	};
}

