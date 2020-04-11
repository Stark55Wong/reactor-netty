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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import reactor.netty.http.Cookies;
import reactor.netty.http.HttpOperations;
import reactor.util.context.Context;

import java.util.Map;
import java.util.Set;

/**
 * @author Violeta Georgieva
 */
final class FailedHttpClientRequest implements HttpClientRequest {

	final Context             context;
	final ClientCookieDecoder cookieDecoder;
	final HttpHeaders         headers;
	final boolean             isWebsocket;
	final HttpMethod          method;
	final String              path;
	final String              uri;

	FailedHttpClientRequest(Context context, HttpClientConfig c) {
		this.context = context;
		this.cookieDecoder = c.cookieDecoder;
		this.headers = c.headers;
		this.isWebsocket = c.websocketSubprotocols != null;
		this.method = c.method;
		this.path = HttpOperations.resolvePath(c.uri);
		this.uri = c.uri;
	}

	@Override
	public HttpClientRequest addCookie(Cookie cookie) {
		throw new UnsupportedOperationException("Should not add Cookie");
	}

	@Override
	public HttpClientRequest addHeader(CharSequence name, CharSequence value) {
		throw new UnsupportedOperationException("Should not add Header");
	}

	@Override
	public Map<CharSequence, Set<Cookie>> cookies() {
		return Cookies.newClientResponseHolder(headers, cookieDecoder)
		              .getCachedCookies();
	}

	@Override
	public Context currentContext() {
		return context;
	}

	@Override
	public String fullPath() {
		return path;
	}

	@Override
	public HttpClientRequest header(CharSequence name, CharSequence value) {
		throw new UnsupportedOperationException("Should not add Header");
	}

	@Override
	public HttpClientRequest headers(HttpHeaders headers) {
		throw new UnsupportedOperationException("Should not add Header");
	}

	@Override
	public boolean isFollowRedirect() {
		return true;
	}

	@Override
	public boolean isKeepAlive() {
		return HttpHeaderValues.CLOSE.contentEqualsIgnoreCase(headers.get(HttpHeaderNames.CONNECTION));
	}

	@Override
	public boolean isWebsocket() {
		return isWebsocket;
	}

	@Override
	public HttpMethod method() {
		return method;
	}

	@Override
	public String[] redirectedFrom() {
		return EMPTY;
	}

	@Override
	public HttpHeaders requestHeaders() {
		return headers;
	}

	@Override
	public String uri() {
		return uri;
	}

	@Override
	public String resourceUrl() {
		return null;
	}

	@Override
	public HttpVersion version() {
		return HttpVersion.HTTP_1_1;
	}

	final static String[] EMPTY = new String[0];
}
