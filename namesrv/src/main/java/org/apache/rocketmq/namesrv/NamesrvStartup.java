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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

/**
 * nameServer启动器
 * @author shixiangqi
 */
public class NamesrvStartup {

    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {

        try {

            //创建nameServer控制器
            //这一步也是启动nameserver的关键，它启动时提供了namesrvConfig和nettyServerConfig配置对象，以及创建NamesrvController核心控制器
            NamesrvController controller = createNamesrvController(args);

            //启动流程控制，对核心控制器进行初始化操作，同时注册一个钩子函数，用于JVM进程关闭时，优雅地释放netty服务、线程池等资源
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {

        //设置remoting模块的version = mqVersion
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        // 这里使用了Apache Commons CLI 命令行解析工具，它可以帮助开发者快速构建启动命令，组织命令的参数、以及输出列表等
        // 创建命令行参数对象，定义-h参数（help） -n参数 =（nameServer）
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        // 根据Options和运行时参数args生成命令行对象，buildCommandlineOptions定义了-c参数（Name server config properties file）和-p参数（Print all config item）
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);
        if (commandLine.hasOption('c')) {
            // 命令行包含 -c 参数解析 nameServer的配置
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                //加载到属性对象
                properties.load(in);

                //装载配置，从properties解析到配置对象namesrvConfig和nettyServerConfig
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                //这个下面的new NamesrvController的时候会check
                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        if (commandLine.hasOption('p')) {
            //包含-p的话打印配置之后退出
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }

        //装载命令行的配置到namesrvConfig
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        if (null == namesrvConfig.getRocketmqHome()) {
            //检查装配好的namesrvConfig配置中必须包含rocketmqHome
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        //输出rocketmqHome的配置信息
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        //日志打印配置信息
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

        //创建NamesrvController对象，k-v配置，路由信息都在这里初始化new出来的
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        //重新注册一遍，防止丢失
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        //上一步create的nameServerController的初始化
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        //加一个钩子函数，shutdown的时候释放netty服务，线程池资源
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                controller.shutdown();
                return null;
            }
        }));

        //启动controller
        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
