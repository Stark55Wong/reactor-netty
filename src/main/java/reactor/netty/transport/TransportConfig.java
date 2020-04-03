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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import reactor.netty.ChannelPipelineConfigurer;
import reactor.netty.ConnectionObserver;
import reactor.netty.NettyPipeline;
import reactor.netty.channel.ChannelMetricsRecorder;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.resources.LoopResources;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.annotation.Nullable;

import static reactor.netty.ReactorNetty.format;

/**
 * A basic configuration holder. The public API is read-only.
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public abstract class TransportConfig {

	/**
	 * Return the read-only default channel attributes
	 *
	 * @return the read-only default channel attributes
	 */
	public final Map<AttributeKey<?>, ?> attributes() {
		if (attrs.isEmpty()) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(attrs);
	}

	/**
	 * Return the local {@link SocketAddress} that will be bound or null
	 *
	 * @return the {@link SocketAddress}
	 */
	@Nullable
	public final Supplier<? extends SocketAddress> bindAddress() {
		return this.bindAddress;
	}

	/**
	 * Return the configured {@link ChannelGroup} or null
	 *
	 * @return the configured {@link ChannelGroup} or null
	 */
	@Nullable
	public final ChannelGroup channelGroup() {
		return channelGroup;
	}

	/**
	 * Return the {@link ChannelInitializer} that will be used for initializing the channel pipeline
	 *
	 * @param connectionObserver the configured {@link ConnectionObserver}
	 * @param remoteAddress the {@link SocketAddress} to connect to, or null when binding a channel
	 * @return the {@link ChannelInitializer} that will be used for initializing the channel pipeline
	 */
	public final ChannelInitializer<Channel> channelInitializer(ConnectionObserver connectionObserver,
			@Nullable SocketAddress remoteAddress, boolean onServer) {
		return new TransportChannelInitializer(channelOperationsProvider(), connectionObserver,
				defaultOnChannelInit().then(doOnChannelInit()), loggingHandler(), metricsRecorder(), remoteAddress, onServer);
	}

	/**
	 * Return the associated {@link ChannelOperations.OnSetup}
	 *
	 * @return the associated {@link ChannelOperations.OnSetup}
	 */
	public ChannelOperations.OnSetup channelOperationsProvider() {
		return ChannelOperations.OnSetup.empty();
	}

	/**
	 * Return the configured {@link ConnectionObserver} if any or {@link ConnectionObserver#emptyListener()}
	 *
	 * @return the configured {@link ConnectionObserver} if any or {@link ConnectionObserver#emptyListener()}
	 */
	public final ConnectionObserver connectionObserver() {
		return observer;
	}

	/**
	 * Return the configured {@link ChannelPipelineConfigurer} if any or {@link ChannelPipelineConfigurer#emptyConfigurer()}
	 *
	 * @return the configured {@link ChannelPipelineConfigurer} if any or {@link ChannelPipelineConfigurer#emptyConfigurer()}
	 */
	public final ChannelPipelineConfigurer doOnChannelInit() {
		return doOnChannelInit;
	}

	/**
	 * Return {@code true} if prefer native event loop and channel factory (e.g. epoll or kqueue)
	 *
	 * @return {@code true} if prefer native event loop and channel factory (e.g. epoll or kqueue)
	 */
	public final boolean isPreferNative() {
		return this.preferNative;
	}

	/**
	 * Return the configured {@link LoggingHandler} or null
	 *
	 * @return the configured {@link LoggingHandler} or null
	 */
	@Nullable
	public final LoggingHandler loggingHandler() {
		return loggingHandler;
	}

	/**
	 * Return the configured {@link LoopResources} or the default
	 *
	 * @return the configured  {@link LoopResources} or the default
	 */
	public final LoopResources loopResources() {
		return loopResources != null ? loopResources : defaultLoopResources();
	}

	/**
	 * Return the configured metrics recorder {@link ChannelMetricsRecorder} or null
	 *
	 * @return the configured metrics recorder {@link ChannelMetricsRecorder} or null
	 */
	@Nullable
	public final Supplier<? extends ChannelMetricsRecorder> metricsRecorder() {
		return  this.metricsRecorder;
	}

	/**
	 * Return the read-only {@link ChannelOption} map
	 *
	 * @return the read-only {@link ChannelOption} map
	 */
	public final Map<ChannelOption<?>, ?> options() {
		if (options.isEmpty()) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(options);
	}


	// Protected/Package private write API

	Map<AttributeKey<?>, ?>                    attrs;
	Supplier<? extends SocketAddress>          bindAddress;
	ChannelGroup                               channelGroup;
	ChannelPipelineConfigurer                  doOnChannelInit;
	LoggingHandler                             loggingHandler;
	LoopResources                              loopResources;
	Supplier<? extends ChannelMetricsRecorder> metricsRecorder;
	ConnectionObserver                         observer;
	Map<ChannelOption<?>, ?>                   options;
	boolean                                    preferNative;

	/**
	 * Default TransportConfig with options
	 */
	protected TransportConfig(Map<ChannelOption<?>, ?> options) {
		this.attrs = Collections.emptyMap();
		this.doOnChannelInit = ChannelPipelineConfigurer.emptyConfigurer();
		this.observer = ConnectionObserver.emptyListener();
		this.options = Objects.requireNonNull(options, "options");
		this.preferNative = LoopResources.DEFAULT_NATIVE;
	}

	/**
	 * Default TransportConfig with options
	 */
	protected TransportConfig(Map<ChannelOption<?>, ?> options, Supplier<? extends SocketAddress> bindAddress) {
		this.attrs = Collections.emptyMap();
		this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress");
		this.doOnChannelInit = ChannelPipelineConfigurer.emptyConfigurer();
		this.observer = ConnectionObserver.emptyListener();
		this.options = Objects.requireNonNull(options, "options");
		this.preferNative = LoopResources.DEFAULT_NATIVE;
	}

	/**
	 * Create TransportConfig from an existing one
	 */
	protected TransportConfig(TransportConfig parent) {
		this.attrs = parent.attrs;
		this.bindAddress = parent.bindAddress;
		this.channelGroup = parent.channelGroup;
		this.doOnChannelInit = parent.doOnChannelInit;
		this.loggingHandler = parent.loggingHandler;
		this.loopResources = parent.loopResources;
		this.metricsRecorder = parent.metricsRecorder;
		this.observer = parent.observer;
		this.options = parent.options;
		this.preferNative = parent.preferNative;
	}

	/**
	 * Return the {@link ChannelFactory} which is used to create {@link Channel} instances.
	 *
	 * @return the {@link ChannelFactory} which is used to create {@link Channel} instances.
	 */
	protected abstract ChannelFactory<? extends Channel> connectionFactory(EventLoopGroup elg);

	/**
	 * Return the configured default {@link ConnectionObserver}.
	 *
	 * @return the configured default {@link ConnectionObserver}
	 */
	protected abstract ConnectionObserver defaultConnectionObserver();

	/**
	 * Return the default {@link LoggingHandler} to wiretap this transport
	 *
	 * @return the default {@link LoggingHandler} to wiretap this transport
	 */
	protected abstract LoggingHandler defaultLoggingHandler();

	/**
	 * Return the default {@link LoopResources} for this transport
	 *
	 * @return the default {@link LoopResources} for this transport
	 */
	protected abstract LoopResources defaultLoopResources();

	/**
	 * Return the configured metrics recorder {@link String}.
	 *
	 * @return the configured metrics category {@link String}
	 */
	protected abstract ChannelMetricsRecorder defaultMetricsRecorder();

	/**
	 * Return the default {@link ChannelPipelineConfigurer} for this transport.
	 *
	 * @return the default {@link ChannelPipelineConfigurer} for this transport
	 */
	protected abstract ChannelPipelineConfigurer defaultOnChannelInit();

	/**
	 * Return the configured {@link EventLoopGroup}.
	 *
	 * @return the configured {@link EventLoopGroup}
	 */
	protected abstract EventLoopGroup eventLoopGroup();

	/**
	 * Add or remove values to a map in an immutable way by returning a new map instance.
	 *
	 * @param parentMap the container map to update
	 * @param key the key to update
	 * @param value the new value or null to remove an existing key
	 * @param <K> key type to add
	 * @param <V> value to add
	 * @return a new instance of the map
	 */
	@SuppressWarnings("unchecked")
	protected static <K, V> Map<K, V> updateMap(Map<K, V> parentMap, Object key, @Nullable Object value) {
		if (parentMap.isEmpty()) {
			return value == null ? parentMap : Collections.singletonMap((K) key, (V) value);
		}
		else {
			Map<K, V> attrs = new HashMap<>(parentMap.size() + 1);
			attrs.putAll(parentMap);
			if (value == null) {
				attrs.remove(key);
			}
			else {
				attrs.put((K) key, (V) value);
			}
			return attrs;
		}
	}

	static final class TransportChannelInitializer extends ChannelInitializer<Channel> {

		final ChannelOperations.OnSetup channelOperationsProvider;
		final ConnectionObserver connectionObserver;
		final ChannelPipelineConfigurer configurer;
		final LoggingHandler loggingHandler;
		final boolean onServer;
		final Supplier<? extends ChannelMetricsRecorder> recorder;
		final SocketAddress remoteAddress;

		TransportChannelInitializer(ChannelOperations.OnSetup channelOperationsProvider, ConnectionObserver connectionObserver,
				ChannelPipelineConfigurer configurer, LoggingHandler loggingHandler,
				Supplier<? extends ChannelMetricsRecorder> recorder, @Nullable SocketAddress remoteAddress, boolean onServer) {
			this.channelOperationsProvider = channelOperationsProvider;
			this.connectionObserver = connectionObserver;
			this.configurer = configurer;
			this.loggingHandler = loggingHandler;
			this.onServer = onServer;
			this.recorder = recorder;
			this.remoteAddress = remoteAddress;
		}

		@Override
		protected void initChannel(Channel channel) {
			ChannelPipeline pipeline = channel.pipeline();

			if (recorder != null) {
				ChannelOperations.addMetricsHandler(channel,
						Objects.requireNonNull(recorder.get(), "Metrics recorder supplier returned null"),
						remoteAddress, onServer);
			}

			if (loggingHandler != null) {
				pipeline.addFirst(NettyPipeline.LoggingHandler, loggingHandler);
			}

			ChannelOperations.addReactiveBridge(channel, channelOperationsProvider, connectionObserver);

			if (configurer != ChannelPipelineConfigurer.emptyConfigurer()) {
				configurer.onChannelInit(channel, connectionObserver, remoteAddress);
			}

			pipeline.remove(this);

			if (log.isDebugEnabled()) {
				log.debug(format(channel, "Initialized pipeline {}"), pipeline.toString());
			}
		}
	}

	static final Logger log = Loggers.getLogger(TransportClientConfig.class);
}