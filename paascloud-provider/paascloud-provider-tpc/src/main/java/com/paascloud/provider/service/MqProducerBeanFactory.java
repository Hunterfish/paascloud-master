/*
 * Copyright (c) 2018. paascloud.net All Rights Reserved.
 * 项目名称：paascloud快速搭建企业级分布式微服务平台
 * 类名称：MqProducerBeanFactory.java
 * 创建人：刘兆明
 * 联系方式：paascloud.net@gmail.com
 * 开源地址: https://github.com/paascloud
 * 博客地址: http://blog.paascloud.net
 * 项目官网: http://paascloud.net
 */

package com.paascloud.provider.service;

import com.google.common.base.Preconditions;
import com.paascloud.core.registry.base.ReliableMessageRegisterDto;
import com.paascloud.core.support.SpringContextHolder;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The class Mq producer bean factory.
 * 生产者端
 * @author paascloud.net @gmail.com
 */
public class MqProducerBeanFactory {

	private MqProducerBeanFactory() {
	}

	private static final ConcurrentHashMap<String, DefaultMQProducer> DEFAULT_MQ_PRODUCER_MAP = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, String> CONSUMER_STATUS_MAP = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, String> PRODUCER_STATUS_MAP = new ConcurrentHashMap<>();

	/**
	 * Gets bean.
	 *
	 * @param pid the pid
	 *
	 * @return the bean
	 */
	public static DefaultMQProducer getBean(String pid) {
		Preconditions.checkArgument(StringUtils.isNotEmpty(pid), "getBean() pid is null");
		DefaultMQProducer producer = DEFAULT_MQ_PRODUCER_MAP.get(pid);
		producer.setVipChannelEnabled(false);
		return producer;
	}

	/**
	 * Build producer bean.
	 *
	 * @param producerDto the producer dto
	 */
	public static void buildProducerBean(ReliableMessageRegisterDto producerDto) {

		String pid = producerDto.getProducerGroup();
		DefaultMQProducer mQProducer = DEFAULT_MQ_PRODUCER_MAP.get(pid);
		if (mQProducer == null) {
			String simpleName = producerDto.getProducerGroup();
			BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultMQProducer.class);
			beanDefinitionBuilder.setScope(BeanDefinition.SCOPE_SINGLETON);
			// DefaultMQProducer 两个重要属性
			// 1. nameSrvAddr: nameServer地址，用户获得broker信息
			// 2. producerGroup: 生产者集合，在同一个producerGroup中有不同的producer实例，
			// 如果最早一个producer崩溃，则broker会通知该组内的其他producer实例进行事务提交或回滚
			beanDefinitionBuilder.addPropertyValue("producerGroup", producerDto.getProducerGroup());
			beanDefinitionBuilder.addPropertyValue("namesrvAddr", producerDto.getNamesrvAddr());
			beanDefinitionBuilder.setInitMethodName("start");
			beanDefinitionBuilder.setDestroyMethodName("shutdown");
			SpringContextHolder.getDefaultListableBeanFactory().registerBeanDefinition(simpleName, beanDefinitionBuilder.getBeanDefinition());
			DEFAULT_MQ_PRODUCER_MAP.put(simpleName, SpringContextHolder.getBean(simpleName));
		}
	}

	public static void putCid(String cid) {
		CONSUMER_STATUS_MAP.put(cid, cid);
	}

	public static void rmCid(String cid) {
		CONSUMER_STATUS_MAP.remove(cid);
	}

	public static void putPid(final String pid) {
		PRODUCER_STATUS_MAP.put(pid, pid);
	}

	public static void rmPid(String pid) {
		PRODUCER_STATUS_MAP.remove(pid);
	}
}
