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
package reactor.netty.tcp;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ReflectiveChannelFactory;
import io.netty.handler.logging.LoggingHandler;
import reactor.netty.ChannelPipelineConfigurer;
import reactor.netty.ReactorNetty;
import reactor.netty.channel.ChannelMetricsRecorder;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.channel.MicrometerChannelMetricsRecorder;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportClientConfig;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Encapsulate all necessary configuration for TCP client transport.
 *
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public final class TcpClientConfig extends TransportClientConfig<TcpClientConfig> {
	SslProvider sslProvider;

	TcpClientConfig(ConnectionProvider connectionProvider, Map<ChannelOption<?>, ?> options,
			Supplier<? extends SocketAddress> remoteAddress) {
		super(connectionProvider, options, remoteAddress);
	}

	TcpClientConfig(TcpClientConfig parent) {
		super(parent);
		this.sslProvider = parent.sslProvider;
	}

	@Override
	public int channelHash() {
		return Objects.hash(super.channelHash(), sslProvider);
	}

	@Override
	public ChannelOperations.OnSetup channelOperationsProvider() {
		return DEFAULT_OPS;
	}

	/**
	 * Return true if that {@link TcpClient} secured via SSL transport
	 *
	 * @return true if that {@link TcpClient} secured via SSL transport
	 */
	public final boolean isSecure(){
		return sslProvider != null;
	}

	/**
	 * Return the current {@link SslProvider} if that {@link TcpClient} secured via SSL
	 * transport or null
	 *
	 * @return the current {@link SslProvider} if that {@link TcpClient} secured via SSL
	 * transport or null
	 */
	@Nullable
	public SslProvider sslProvider(){
		return sslProvider;
	}

	@Override
	protected ChannelFactory<? extends Channel> connectionFactory(EventLoopGroup elg) {
		return new ReflectiveChannelFactory<>(loopResources().onChannel(elg));
	}

	@Override
	protected ChannelPipelineConfigurer defaultOnChannelInit() {
		return super.defaultOnChannelInit()
				.then((channel, observer, remoteAddress) -> {
					SslProvider sslProvider = sslProvider();
					if (sslProvider != null) {
						sslProvider.addSslHandler(channel, remoteAddress, SSL_DEBUG);
					}
				});
	}

	@Override
	protected LoggingHandler defaultLoggingHandler() {
		return LOGGING_HANDLER;
	}

	@Override
	protected LoopResources defaultLoopResources() {
		return TcpResources.get();
	}

	@Override
	protected ChannelMetricsRecorder defaultMetricsRecorder() {
		return MicrometerTcpClientMetricsRecorder.INSTANCE;
	}

	@Override
	protected EventLoopGroup eventLoopGroup() {
		return loopResources().onClient(isPreferNative());
	}

	static final ChannelOperations.OnSetup DEFAULT_OPS = (ch, c, msg) -> new ChannelOperations<>(ch, c);

	static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(TcpClient.class);

	/**
	 * Default value whether the SSL debugging on the client side will be enabled/disabled,
	 * fallback to SSL debugging disabled
	 */
	static final boolean SSL_DEBUG = Boolean.parseBoolean(System.getProperty(ReactorNetty.SSL_CLIENT_DEBUG, "false"));

	static final class MicrometerTcpClientMetricsRecorder extends MicrometerChannelMetricsRecorder {

		static final MicrometerTcpClientMetricsRecorder INSTANCE =
				new MicrometerTcpClientMetricsRecorder(reactor.netty.Metrics.TCP_CLIENT_PREFIX, "tcp");

		MicrometerTcpClientMetricsRecorder(String name, String protocol) {
			super(name, protocol);
		}
	}
}
