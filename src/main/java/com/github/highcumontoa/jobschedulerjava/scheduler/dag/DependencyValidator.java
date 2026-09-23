package com.github.highcumontoa.jobschedulerjava.scheduler.dag;

import com.github.highcumontoa.jobschedulerjava.scheduler.model.FailureReason;
import com.github.highcumontoa.jobschedulerjava.scheduler.model.TaskDefinition;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 依赖图校验：重复定义、未知依赖、成环检测。
 * 校验顺序固定为 重复定义 -> 未知依赖 -> 成环，保证同一非法输入
 * 总是得到同一个可区分原因。
 */
public class DependencyValidator {

    /**
     * 校验一批任务定义（连同系统内已存在的任务 id）。
     *
     * @param batch       本批次提交的任务
     * @param existingIds 系统内已存在的任务 id
     */
    public ValidationResult validate(Collection<TaskDefinition> batch, Set<String> existingIds) {
        // 1. 重复定义：批次内部重复 或 与已存在任务重复
        Set<String> seen = new HashSet<>(existingIds);
        for (TaskDefinition def : batch) {
            if (!seen.add(def.taskId())) {
                return ValidationResult.reject(FailureReason.DUPLICATE_TASK,
                        "taskId duplicated: " + def.taskId());
            }
        }

        // 2. 未知依赖：依赖必须指向本批次或已存在的任务，且不允许自依赖
        Map<String, TaskDefinition> byId = new HashMap<>();
        for (TaskDefinition def : batch) {
            byId.put(def.taskId(), def);
        }
        for (TaskDefinition def : batch) {
            for (String dep : def.dependencies()) {
                if (dep.equals(def.taskId())) {
                    return ValidationResult.reject(FailureReason.DEPENDENCY_CYCLE,
                            "self dependency: " + def.taskId());
                }
                if (!seen.contains(dep)) {
                    return ValidationResult.reject(FailureReason.UNKNOWN_DEPENDENCY,
                            "task " + def.taskId() + " depends on unknown task: " + dep);
                }
            }
        }

        // 3. 成环检测：仅在本批次新增边上做三色标记 DFS
        //    （已存在任务之间的依赖关系历史上已校验过，无需重复检测）
        Map<String, Integer> color = new HashMap<>(); // 0=未访问 1=访问中 2=已完成
        for (TaskDefinition def : batch) {
            color.put(def.taskId(), 0);
        }
        for (TaskDefinition def : batch) {
            if (color.get(def.taskId()) == 0) {
                String cycle = dfsFindCycle(def.taskId(), byId, color);
                if (cycle != null) {
                    return ValidationResult.reject(FailureReason.DEPENDENCY_CYCLE,
                            "dependency cycle detected: " + cycle);
                }
            }
        }
        return ValidationResult.passed();
    }

    private String dfsFindCycle(String start, Map<String, TaskDefinition> byId,
                                Map<String, Integer> color) {
        Deque<String> path = new ArrayDeque<>();
        return dfs(start, byId, color, path);
    }

    private String dfs(String node, Map<String, TaskDefinition> byId,
                       Map<String, Integer> color, Deque<String> path) {
        color.put(node, 1);
        path.addLast(node);
        TaskDefinition def = byId.get(node);
        if (def != null) {
            for (String dep : def.dependencies()) {
                Integer c = color.get(dep);
                if (c == null) {
                    continue; // 指向已存在任务，历史上无环
                }
                if (c == 1) {
                    // 找到环：拼接环路径便于解释
                    StringBuilder sb = new StringBuilder();
                    boolean inCycle = false;
                    for (String p : path) {
                        if (p.equals(dep)) {
                            inCycle = true;
                        }
                        if (inCycle) {
                            sb.append(p).append(" -> ");
                        }
                    }
                    sb.append(dep);
                    return sb.toString();
                }
                if (c == 0) {
                    String cycle = dfs(dep, byId, color, path);
                    if (cycle != null) {
                        return cycle;
                    }
                }
            }
        }
        path.removeLast();
        color.put(node, 2);
        return null;
    }
}
