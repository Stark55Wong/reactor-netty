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
package reactor.netty;

import io.netty.channel.Channel;

import javax.annotation.Nullable;
import java.net.SocketAddress;

/**
 * Configure the channel pipeline while initializing the channel.
 *
 * @author Violeta Georgieva
 */
@FunctionalInterface
public interface ChannelPipelineConfigurer {

	/**
	 * Return a noop channel pipeline configurer
	 *
	 * @return a noop channel pipeline configurer
	 */
	static ChannelPipelineConfigurer emptyConfigurer(){
		return ReactorNetty.NOOP_CONFIGURER;
	}

	/**
	 * Configure the channel pipeline while initializing the channel.
	 *
	 * @param channel the channel
	 * @param connectionObserver the configured {@link ConnectionObserver}
	 * @param remoteAddress the {@link SocketAddress} to connect to, or null when binding a channel
	 */
	void onChannelInit(Channel channel, ConnectionObserver connectionObserver, @Nullable SocketAddress remoteAddress);

	/**
	 * Chain together another {@link ChannelPipelineConfigurer}
	 *
	 * @param other the next {@link ChannelPipelineConfigurer}
	 * @return a new composite {@link ChannelPipelineConfigurer}
	 */
	default ChannelPipelineConfigurer then(ChannelPipelineConfigurer other) {
		return ReactorNetty.compositeChannelPipelineConfigurer(this, other);
	}
}
