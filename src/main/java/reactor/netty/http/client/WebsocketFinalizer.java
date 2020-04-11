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
import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

/**
 * @author Stephane Maldini
 * @author Violeta Georgieva
 */
final class WebsocketFinalizer implements HttpClient.WebsocketSender {

	final HttpClientConnect httpClient;

	WebsocketFinalizer(HttpClientConnect httpClient) {
		this.httpClient = httpClient;
	}

	// UriConfiguration methods

	@Override
	public HttpClient.WebsocketSender uri(Mono<String> uri) {
		Objects.requireNonNull(uri, "uri");
		HttpClient dup = httpClient.duplicate();
		dup.configuration().deferredConf(client -> uri.map(s -> {
			client.config.uri = s;
			return client;
		}));
		return new WebsocketFinalizer((HttpClientConnect) dup);
	}

	@Override
	public HttpClient.WebsocketSender uri(String uri) {
		Objects.requireNonNull(uri, "uri");
		HttpClient dup = httpClient.duplicate();
		dup.configuration().uri = uri;
		return new WebsocketFinalizer((HttpClientConnect) dup);
	}

	// WebsocketSender methods

	@Override
	public WebsocketFinalizer send(Function<? super HttpClientRequest, ? extends Publisher<Void>> sender) {
		Objects.requireNonNull(sender, "requestBody");
		HttpClient dup = httpClient.duplicate();
		dup.configuration().body = (req, out) -> sender.apply(req);
		return new WebsocketFinalizer((HttpClientConnect) dup);
	}

	// WebsocketReceiver methods

	@Override
	@SuppressWarnings("unchecked")
	public Mono<WebsocketClientOperations> connect() {
		return (Mono<WebsocketClientOperations>) httpClient.connect();
	}

	@Override
	public <V> Flux<V> handle(BiFunction<? super WebsocketInbound, ? super WebsocketOutbound, ? extends Publisher<V>> receiver) {
		return connect().flatMapMany(c -> Flux.from(receiver.apply(c, c))
		                                      .doFinally(s -> HttpClientFinalizer.discard(c)));
	}

	@Override
	public ByteBufFlux receive() {
		return HttpClientFinalizer.content(httpClient);
	}
}

