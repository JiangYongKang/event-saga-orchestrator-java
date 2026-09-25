# event-saga-orchestrator-java

单进程内可复现的事件驱动流程编排（Saga）与补偿能力。不依赖外部消息中间件或数据库，
所有状态保存在进程内存中，重复、乱序、并发与中途失败下的最终状态可预期、可解释。

## 快速开始

```bash
# 运行测试（覆盖重复投递 / 乱序 / 并发 / 补偿失败等场景）
mvn test

# 本地启动（随机端口，见控制台输出）
mvn spring-boot:run
```

启动后内置示例流程定义 `order-fulfillment`（预留库存 → 扣款 → 创建发货单）：

```bash
# 1. 创建流程（businessKey 为业务键，重复创建幂等返回同一实例）
curl -X POST http://localhost:$PORT/api/sagas \
  -H 'Content-Type: application/json' \
  -d '{"definitionName":"order-fulfillment","businessKey":"order-1001"}'

# 2. 逐步投递事件驱动推进（eventId 需全局唯一，作为幂等键）
curl -X POST http://localhost:$PORT/api/sagas/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-1","businessKey":"order-1001","stepName":"reserve-inventory","type":"STEP_SUCCEEDED"}'

# 3. 查询状态
curl http://localhost:$PORT/api/sagas/{sagaId}
```

## 事件契约

每一步推进都必须由事件驱动，事件结构见 `SagaEvent`：

| 字段 | 说明 |
| --- | --- |
| `eventId` | 全局唯一，幂等键。同一 `eventId` 重复投递不产生重复副作用 |
| `businessKey` | 业务键，定位流程实例；同一业务键的操作被串行化 |
| `stepName` | 事件对应的步骤名，必须等于当前待推进步骤，否则判为乱序 |
| `type` | `STEP_SUCCEEDED`（推进）/ `STEP_FAILED`（触发补偿） |
| `detail` / `occurredAt` | 审计信息 |

事件处理回执 `EventAck` 包含可区分的原因（`AckReason`）：

| 原因 | 含义 |
| --- | --- |
| `APPLIED` | 已应用并产生状态迁移 |
| `DUPLICATE_EVENT` | 相同 `eventId` 已处理过，未重复产生副作用 |
| `OUT_OF_ORDER` | 事件步骤不是当前待推进步骤（乱序/过期），状态不变 |
| `IGNORED_TERMINAL_STATE` | 流程已处终态，事件被忽略 |
| `REJECTED_UNKNOWN_STEP` | 步骤名不属于该流程定义 |

## 状态机与补偿规则

```
NEW --STEP_SUCCEEDED--> RUNNING --全部步骤完成--> COMPLETED（终态）
NEW/RUNNING --STEP_FAILED 或正向动作异常--> COMPENSATING
COMPENSATING --全部补偿成功--> COMPENSATED（终态）
COMPENSATING --补偿重试耗尽--> MANUAL_INTERVENTION（终态，人工介入）
```

- 步骤按定义顺序推进；`STEP_SUCCEEDED` 被接受时执行该步骤的正向动作并前进一格。
- 失败时按**与执行相反的顺序**补偿所有已完成步骤（`EXECUTED` → `COMPENSATED`）。
- 补偿失败自动重试，最多 `MAX_COMPENSATION_ATTEMPTS = 3` 次（含首次）；
  重试耗尽后流程进入 `MANUAL_INTERVENTION` 终态并记录补偿尝试次数，
  **不静默丢弃、不停留在中间状态**。
- 终态（`COMPLETED` / `COMPENSATED` / `MANUAL_INTERVENTION`）不再接受任何推进事件。

每次状态迁移都会在日志中打印依据，例如：

```
[state] sagaId=... 状态迁移 RUNNING -> COMPENSATING，依据：触发补偿，按相反顺序回滚
[compensate] 步骤 charge-payment 第 2 次补偿成功
```

## 幂等、并发与重试策略

- **幂等**：已应用的 `eventId` 记录在进程内事件表中，重复投递返回 `DUPLICATE_EVENT`，
  副作用（步骤动作/补偿）恰好执行一次。同一 `businessKey` 重复创建返回已有实例。
- **并发与乱序**：同一业务键的所有操作在同一监视器锁下串行执行；并发投递同一
  步骤的多个事件时只有一个被应用，其余判为重复或乱序，不会重复推进或状态回退。
- **错误归一化**：对外仅暴露 `SagaException(category, message)`，类别包括
  `VALIDATION / NOT_FOUND / CONFLICT / TIMEOUT / RETRY_EXHAUSTED / DEAD_LETTER / INTERNAL`；
  内部异常细节（堆栈、原因）只写日志，不泄露给调用方。REST 层由
  `ApiExceptionHandler` 统一映射为 `{category, message}`。

## 本地验证

```bash
mvn test
```

测试场景（`SagaOrchestratorTest`）：

| 用例 | 断言 |
| --- | --- |
| `happyPathReachesCompleted` | 三步顺序推进后状态为 `COMPLETED`，动作各执行 1 次、无补偿 |
| `duplicateDeliveryDoesNotRepeatSideEffects` | 同一事件投递 3 次，仅首次 `APPLIED`，动作只执行 1 次 |
| `outOfOrderEventIsRejectedWithoutStateChange` | 乱序事件返回 `OUT_OF_ORDER`，状态与副作用均不变 |
| `concurrentAdvancementAppliesExactlyOncePerStep` | 16 线程并发推进，每步恰好应用 1 次，最终 `COMPLETED` |
| `failedStepTriggersReverseCompensation` | 第 3 步失败后已完成的 1、2 步各补偿 1 次，状态 `COMPENSATED` |
| `compensationRetriesUntilSuccess` | 补偿失败 2 次后第 3 次成功，尝试次数为 3 |
| `compensationExhaustionLeadsToManualInterventionTerminalState` | 补偿持续失败 → `MANUAL_INTERVENTION`，尝试次数=3，终态事件被忽略 |
| `concurrentCreateWithSameBusinessKeyReturnsSameSaga` | 并发创建同一业务键收敛到同一实例 |

## 工程结构

```
src/main/java/com/github/highcumontoa/saga/
├── domain/    # 状态枚举、流程/步骤定义与实例
├── event/     # 事件契约、回执与可区分原因
├── error/     # 归一化错误类别与异常
├── store/     # 进程内实例存储 + 已处理事件表
├── core/      # 编排器（状态机、串行化、补偿与重试）
├── api/       # REST 入口与统一错误出口
└── demo/      # 示例流程定义 order-fulfillment
```
