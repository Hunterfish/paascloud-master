package com.paascloud.core.registry;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.paascloud.config.properties.AliyunProperties;
import com.paascloud.config.properties.PaascloudProperties;
import com.paascloud.config.properties.ZookeeperProperties;
import com.paascloud.core.generator.IncrementIdGenerator;
import com.paascloud.core.registry.base.CoordinatorRegistryCenter;
import com.paascloud.core.registry.base.RegisterDto;
import com.paascloud.core.registry.zookeeper.ZookeeperRegistryCenter;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.concurrent.ConcurrentHashMap;





/**
 * 注册中心工厂.
 *
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RegistryCenterFactory {

	private static final ConcurrentHashMap<HashCode, CoordinatorRegistryCenter> REG_CENTER_REGISTRY = new ConcurrentHashMap<>();

	/**
	 * 创建注册中心.
	 *
	 * @param zookeeperProperties the zookeeper properties
	 *
	 * @return 注册中心对象 coordinator registry center
	 */
	public static CoordinatorRegistryCenter createCoordinatorRegistryCenter(ZookeeperProperties zookeeperProperties) {
		Hasher hasher = Hashing.md5().newHasher().putString(zookeeperProperties.getZkAddressList(), Charsets.UTF_8);
		HashCode hashCode = hasher.hash();
		CoordinatorRegistryCenter result = REG_CENTER_REGISTRY.get(hashCode);
		if (null != result) {
			return result;
		}
		result = new ZookeeperRegistryCenter(zookeeperProperties);
		// 生成 zookeeper 客户端操作类 CuratorFramework
		result.init();
		REG_CENTER_REGISTRY.put(hashCode, result);
		return result;
	}

	/**
	 * Startup.
	 *
	 * @param paascloudProperties the paascloud properties
	 * @param host                the host
	 * @param app                 the app
	 */
	public static void startup(PaascloudProperties paascloudProperties, String host, String app) {
		// 1. 初始化用于协调分布式服务的注册中心
		CoordinatorRegistryCenter coordinatorRegistryCenter = createCoordinatorRegistryCenter(paascloudProperties.getZk());
		RegisterDto dto = new RegisterDto(app, host, coordinatorRegistryCenter);
		// 2. 生成该启动服务的分布式 ID
		Long serviceId = new IncrementIdGenerator(dto).nextId();
		IncrementIdGenerator.setServiceId(serviceId);
		registerMq(paascloudProperties, host, app);
	}
	
	/**
	 * 注册 rocketmq 生产者消费者到 zookeeper 中心
	 * @return void
	 */
	private static void registerMq(PaascloudProperties paascloudProperties, String host, String app) {
		CoordinatorRegistryCenter coordinatorRegistryCenter = createCoordinatorRegistryCenter(paascloudProperties.getZk());
		AliyunProperties.RocketMqProperties rocketMq = paascloudProperties.getAliyun().getRocketMq();
		// 判断该服务是生产者还是消费者，还是两者都是
		String consumerGroup = rocketMq.isReliableMessageConsumer() ? rocketMq.getConsumerGroup() : null;
		String namesrvAddr = rocketMq.getNamesrvAddr();
		String producerGroup = rocketMq.isReliableMessageProducer() ? rocketMq.getProducerGroup() : null;
		coordinatorRegistryCenter.registerMq(app, host, producerGroup, consumerGroup, namesrvAddr);
	}
}
