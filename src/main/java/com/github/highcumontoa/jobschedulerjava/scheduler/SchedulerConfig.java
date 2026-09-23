package com.github.highcumontoa.jobschedulerjava.scheduler;

import com.github.highcumontoa.jobschedulerjava.scheduler.core.LocalJobScheduler;
import com.github.highcumontoa.jobschedulerjava.scheduler.dag.DependencyValidator;
import com.github.highcumontoa.jobschedulerjava.scheduler.quota.QuotaManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 调度器 Spring 装配：默认提供一个本地队列配额配置，
 * 业务方可通过自定义 QuotaManager Bean 覆盖。
 */
@Configuration
public class SchedulerConfig {

    @Bean
    public QuotaManager quotaManager() {
        QuotaManager manager = new QuotaManager();
        // 默认队列：并发 4、资源 8，配额不足时排队等待
        manager.configureQueue("default", 4, 8, false);
        return manager;
    }

    @Bean
    public DependencyValidator dependencyValidator() {
        return new DependencyValidator();
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public LocalJobScheduler localJobScheduler(QuotaManager quotaManager,
                                               DependencyValidator validator) {
        return new LocalJobScheduler(quotaManager, validator);
    }
}
