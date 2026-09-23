package com.github.highcumontoa.jobschedulerjava.scheduler.quota;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按队列维度的并发数与资源配额管理。
 * 所有方法线程安全；tryAcquire/release 必须配对，任何并发下不得超发。
 */
public class QuotaManager {

    private static final class QueueState {
        final String queue;
        final int maxConcurrent;
        final int maxResource;
        final boolean rejectWhenFull;
        int running;
        int usedResource;

        QueueState(String queue, int maxConcurrent, int maxResource, boolean rejectWhenFull) {
            this.queue = queue;
            this.maxConcurrent = maxConcurrent;
            this.maxResource = maxResource;
            this.rejectWhenFull = rejectWhenFull;
        }
    }

    private final Map<String, QueueState> queues = new ConcurrentHashMap<>();

    /**
     * 配置队列配额（重复配置会覆盖）。
     *
     * @param rejectWhenFull true 时配额不足应直接拒绝；false 时调用方应排队等待
     */
    public void configureQueue(String queue, int maxConcurrent, int maxResource, boolean rejectWhenFull) {
        if (maxConcurrent <= 0 || maxResource <= 0) {
            throw new IllegalArgumentException("quota limits must be positive");
        }
        queues.put(queue, new QueueState(queue, maxConcurrent, maxResource, rejectWhenFull));
    }

    /**
     * 尝试占用配额。同一队列上的检查与占用是原子的，并发调用不会超发。
     * 未配置的队列视为无限制。
     */
    public boolean tryAcquire(String queue, int cost) {
        QueueState state = queues.get(queue);
        if (state == null) {
            return true;
        }
        synchronized (state) {
            if (state.running + 1 > state.maxConcurrent
                    || state.usedResource + cost > state.maxResource) {
                return false;
            }
            state.running++;
            state.usedResource += cost;
            return true;
        }
    }

    /** 释放配额，必须与成功的 tryAcquire 配对。 */
    public void release(String queue, int cost) {
        QueueState state = queues.get(queue);
        if (state == null) {
            return;
        }
        synchronized (state) {
            state.running = Math.max(0, state.running - 1);
            state.usedResource = Math.max(0, state.usedResource - cost);
        }
    }

    /** 队列是否采用“配额不足即拒绝”策略；未配置队列默认排队。 */
    public boolean isRejectWhenFull(String queue) {
        QueueState state = queues.get(queue);
        return state != null && state.rejectWhenFull;
    }

    /** 所有队列的使用快照。 */
    public List<QueueUsage> snapshot() {
        List<QueueUsage> result = new ArrayList<>();
        for (QueueState state : queues.values()) {
            synchronized (state) {
                result.add(new QueueUsage(state.queue, state.running, state.maxConcurrent,
                        state.usedResource, state.maxResource));
            }
        }
        return result;
    }
}
