/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker;

import java.io.File;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerControllerTest {

    @Test
    public void testBrokerRestart() throws Exception {
        BrokerController brokerController = new BrokerController(
            new BrokerConfig(),
            new NettyServerConfig(),
            new NettyClientConfig(),
            new MessageStoreConfig());
        assertThat(brokerController.initialize());
        brokerController.start();
        brokerController.shutdown();
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(new MessageStoreConfig().getStorePathRootDir()));
    }

    public static void main(String[] args) throws Exception {

        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9302);

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName("broker-001");
        brokerConfig.setNamesrvAddr("127.0.0.1:9301");

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        //固定一个时间，删除过期的消息文件，默认是04，凌晨4点
        messageStoreConfig.setDeleteWhen("02");
        //消息文件保留时长，默认是72小时
        messageStoreConfig.setFileReservedTime(48);
        //刷盘方式，同步会损耗性能，异步极端情况会丢失消息
        messageStoreConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);

        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, new NettyClientConfig(), messageStoreConfig);
        brokerController.initialize();
        brokerController.start();

        Thread.sleep(DateUtils.MILLIS_PER_DAY);
    }
}
