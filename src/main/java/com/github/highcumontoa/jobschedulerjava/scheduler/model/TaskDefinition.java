package com.github.highcumontoa.jobschedulerjava.scheduler.model;

import java.util.List;
import java.util.Set;

/**
 * 一次任务提交的完整定义（不可变）。
 */
public final class TaskDefinition {

    private final String id;
    private final String queue;
    private final TaskBody body;
    private final Set<String> dependencies;
    private final int cpuUnits;
    private final long memoryBytes;
    private final int maxRetries;
    private final long timeoutMillis;
    private final boolean rejectWhenBusy;

    public TaskDefinition(String id, String queue, TaskBody body, Set<String> dependencies,
                          int cpuUnits, long memoryBytes, int maxRetries, long timeoutMillis,
                          boolean rejectWhenBusy) {
        this.id = id;
        this.queue = queue;
        this.body = body;
        this.dependencies = dependencies == null ? Set.of() : Set.copyOf(dependencies);
        this.cpuUnits = cpuUnits;
        this.memoryBytes = memoryBytes;
        this.maxRetries = maxRetries;
        this.timeoutMillis = timeoutMillis;
        this.rejectWhenBusy = rejectWhenBusy;
    }

    public String getId() { return id; }
    public String getQueue() { return queue; }
    public TaskBody getBody() { return body; }
    public Set<String> getDependencies() { return dependencies; }
    public int getCpuUnits() { return cpuUnits; }
    public long getMemoryBytes() { return memoryBytes; }
    public int getMaxRetries() { return maxRetries; }
    public long getTimeoutMillis() { return timeoutMillis; }
    /** 队列满/配额不足时：true 直接拒绝（REJECTED），false 进入排队（QUEUED）。 */
    public boolean isRejectWhenBusy() { return rejectWhenBusy; }

    public static Builder builder(String id) {
        return new Builder(id);
    }

    public static final class Builder {
        private final String id;
        private String queue = "default";
        private TaskBody body = ctx -> { };
        private Set<String> dependencies = Set.of();
        private int cpuUnits = 1;
        private long memoryBytes = 0L;
        private int maxRetries = 0;
        private long timeoutMillis = 0L;
        private boolean rejectWhenBusy = false;

        private Builder(String id) { this.id = id; }

        public Builder queue(String queue) { this.queue = queue; return this; }
        public Builder body(TaskBody body) { this.body = body; return this; }
        public Builder dependsOn(String... deps) { this.dependencies = Set.of(deps); return this; }
        public Builder dependsOn(Set<String> deps) { this.dependencies = deps == null ? Set.of() : Set.copyOf(deps); return this; }
        public Builder cpuUnits(int cpuUnits) { this.cpuUnits = cpuUnits; return this; }
        public Builder memoryBytes(long memoryBytes) { this.memoryBytes = memoryBytes; return this; }
        public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
        public Builder timeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; return this; }
        public Builder rejectWhenBusy(boolean rejectWhenBusy) { this.rejectWhenBusy = rejectWhenBusy; return this; }

        public TaskDefinition build() {
            return new TaskDefinition(id, queue, body, dependencies, cpuUnits, memoryBytes,
                    maxRetries, timeoutMillis, rejectWhenBusy);
        }
    }
}
