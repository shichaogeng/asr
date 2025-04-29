package com.example.asrwebsocket.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * ASR服务配置类
 * 用于加载ASR服务相关的配置属性
 */
@Configuration
@PropertySource(value = "classpath:application.properties", ignoreResourceNotFound = true)
public class AsrConfig {
    // 配置类，用于加载application.properties中的ASR相关配置
    // AsrServiceImpl中的@Value注解会自动注入这些属性
}