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
package reactor.netty.transport;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import reactor.netty.ChannelPipelineConfigurer;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.ReactorNetty;
import reactor.netty.resources.ConnectionProvider;

import javax.annotation.Nullable;

/**
 * Encapsulate all necessary configuration for client transport. The public API is read-only.
 *
 * @param <CONF> Configuration implementation
 * @author Stephane Maldini
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public abstract class TransportClientConfig<CONF extends TransportConfig> extends TransportConfig {

	/**
	 * Return the {@link ConnectionProvider}
	 *
	 * @return the {@link ConnectionProvider}
	 */
	public final ConnectionProvider connectionProvider() {
		return connectionProvider;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super CONF> doOnConnect() {
		return doOnConnect;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super Connection> doOnConnected() {
		return doOnConnected;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super Connection> doOnDisconnected() {
		return doOnDisconnected;
	}

	/**
	 * Return the remote configured {@link SocketAddress}
	 *
	 * @return the remote configured {@link SocketAddress}
	 */
	public final Supplier<? extends SocketAddress> remoteAddress() {
		return remoteAddress;
	}

	/**
	 * Return the {@link AddressResolverGroup}
	 *
	 * @return the {@link AddressResolverGroup}
	 */
	public final AddressResolverGroup<?> resolver() {
		return resolver;
	}


	// Package private creators

	final ConnectionProvider connectionProvider;

	Consumer<? super CONF>            doOnConnect;
	Consumer<? super Connection>      doOnConnected;
	Consumer<? super Connection>      doOnDisconnected;
	Supplier<? extends SocketAddress> remoteAddress;
	AddressResolverGroup<?>           resolver;

	protected TransportClientConfig(ConnectionProvider connectionProvider, Map<ChannelOption<?>, ?> options,
			Supplier<? extends SocketAddress> remoteAddress) {
		super(options);
		this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
		this.remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
		this.resolver = DefaultAddressResolverGroup.INSTANCE;
	}

	protected TransportClientConfig(TransportClientConfig<CONF> parent) {
		super(parent);
		this.connectionProvider = parent.connectionProvider;
		this.doOnConnect = parent.doOnConnect;
		this.doOnConnected = parent.doOnConnected;
		this.doOnDisconnected = parent.doOnDisconnected;
		this.remoteAddress = parent.remoteAddress;
		this.resolver = parent.resolver;
	}

	@Override
	protected ConnectionObserver defaultConnectionObserver() {
		if (channelGroup() == null && doOnConnected() == null && doOnDisconnected() == null) {
			return ConnectionObserver.emptyListener();
		}
		return new TransportClientDoOn(channelGroup(), doOnConnected(), doOnDisconnected());
	}

	@Override
	protected ChannelPipelineConfigurer defaultOnChannelInit() {
		return ChannelPipelineConfigurer.emptyConfigurer();
	}

	static final class TransportClientDoOn implements ConnectionObserver {

		final ChannelGroup channelGroup;
		final Consumer<? super Connection> doOnConnected;
		final Consumer<? super Connection> doOnDisconnected;

		TransportClientDoOn(@Nullable ChannelGroup channelGroup,
				@Nullable Consumer<? super Connection> doOnConnected,
				@Nullable Consumer<? super Connection> doOnDisconnected) {
			this.channelGroup = channelGroup;
			this.doOnConnected = doOnConnected;
			this.doOnDisconnected = doOnDisconnected;
		}

		@Override
		public void onStateChange(Connection connection, State newState) {
			if (channelGroup != null && newState == State.CONNECTED) {
				channelGroup.add(connection.channel());
				return;
			}
			if (doOnConnected != null && newState == State.CONFIGURED) {
				doOnConnected.accept(connection);
				return;
			}
			if (doOnDisconnected != null) {
				if (newState == State.DISCONNECTING) {
					connection.onDispose(() -> doOnDisconnected.accept(connection));
				}
				else if (newState == State.RELEASED) {
					doOnDisconnected.accept(connection);
				}
			}
		}
	}
}