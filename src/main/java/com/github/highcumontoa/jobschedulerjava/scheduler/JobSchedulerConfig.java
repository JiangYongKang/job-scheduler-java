package com.github.highcumontoa.jobschedulerjava.scheduler;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 暴露默认调度器 Bean，便于在 Spring Boot 应用内直接注入使用。
 */
@Configuration
public class JobSchedulerConfig {

    @Bean(destroyMethod = "shutdown")
    public JobScheduler jobScheduler() {
        return new LocalJobScheduler();
    }
}
