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

### 步骤时间预算（超时）

`StepDefinition` 可携带 `timeout`（`Duration`，缺省 `null` 表示不限制，保持既有行为）：

- 设置了时间预算的步骤在独立线程执行并限时等待；超时后中断执行线程，
  **按失败处理并正常走反向补偿**，流程不会一直悬着。
- 超时与普通业务失败**可区分**：步骤记录上的 `failureCategory` 为
  `TIMEOUT`（超时）或 `BUSINESS_FAILURE`（业务异常 / 下游失败事件），
  查询接口 `GET /api/sagas/{sagaId}` 的 `steps[].failureCategory` 对外可见。
- 若超时后的补偿也重试耗尽，则进入人工介入，清单条目类别为 `RETRY_EXHAUSTED`。

每次状态迁移都会在日志中打印依据，例如：

```
[state] sagaId=... 状态迁移 RUNNING -> COMPENSATING，依据：触发补偿，按相反顺序回滚
[compensate] 步骤 charge-payment 第 2 次补偿成功
```

## 人工介入清单

补偿重试耗尽进入 `MANUAL_INTERVENTION` 的流程会登记到人工介入清单
（按 `sagaId` 去重，同一流程至多一条，重复进入只更新不叠加）。条目包含：
卡住的步骤 `stuckStepName`、失败原因 `failureReason`、已补偿次数
`compensationAttempts`、重投次数 `reprocessAttempts`、状态（`PENDING` / `RESOLVED`）
与时间戳。

REST 入口：

```bash
# 查询清单（businessKey / status 均可选，支持组合过滤）
curl "http://localhost:$PORT/api/sagas/interventions?businessKey=order-1001&status=PENDING"

# 人工确认后重新发起补偿处理
curl -X POST http://localhost:$PORT/api/sagas/interventions/{sagaId}/reprocess

# 清理已处理完成（RESOLVED）的条目，避免堆积
curl -X DELETE http://localhost:$PORT/api/sagas/interventions/resolved
```

重新处理（reprocess）规则：

- 仅允许对处于 `MANUAL_INTERVENTION` 终态且条目为 `PENDING` 的流程发起；
  否则返回 `CONFLICT`（条目不存在返回 `NOT_FOUND`）。
- 重投给尚未回滚的步骤一轮新的重试预算（3 次），**已成功回滚的步骤不会
  被重复补偿**，回滚顺序仍保持与原执行顺序相反。
- 重投成功：流程进入 `COMPENSATED` 终态，条目标记 `RESOLVED`，可被清理。
- 重投仍失败：流程回到 `MANUAL_INTERVENTION` 终态（不停留在中间态），
  原条目更新 `reprocessAttempts` 与时间戳，**不叠加新记录**。

## 幂等、并发与重试策略

- **幂等**：已应用的 `eventId` 记录在进程内事件表中，重复投递返回 `DUPLICATE_EVENT`，
  副作用（步骤动作/补偿）恰好执行一次。同一 `businessKey` 重复创建返回已有实例。
- **并发与乱序**：同一业务键的所有操作在同一监视器锁下串行执行；并发投递同一
  步骤的多个事件时只有一个被应用，其余判为重复或乱序，不会重复推进或状态回退。
- **错误归一化**：对外仅暴露 `SagaException(category, message)`，类别包括
  `VALIDATION / NOT_FOUND / CONFLICT / TIMEOUT / BUSINESS_FAILURE / RETRY_EXHAUSTED / DEAD_LETTER / INTERNAL`；
  超时、业务失败、重试耗尽、人工介入四类情况类别稳定且互相区分；
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

新增场景（`SagaTimeoutAndInterventionTest`，运行时日志会打印每个场景名）：

| 用例 | 断言 |
| --- | --- |
| `stepTimeoutTriggersCompensationAndIsDistinguishableFromBusinessFailure` | 超时步骤按失败补偿，`failureCategory=TIMEOUT`，与业务失败 `BUSINESS_FAILURE` 可区分 |
| `timeoutFollowedByCompensationExhaustionCreatesInterventionEntry` | 超时 + 补偿耗尽 → 人工介入，步骤记录 `TIMEOUT`、清单条目 `RETRY_EXHAUSTED` |
| `manualInterventionEntriesAreQueryableByBusinessKeyAndStatus` | 清单按业务键/状态可查，含卡住步骤、失败原因、补偿次数 |
| `reprocessSkipsCompensatedStepsAndKeepsReverseOrder` | 重投不重复补偿已回滚步骤，回滚顺序保持相反 |
| `repeatedReprocessFailureKeepsTerminalStateAndSingleEntry` | 多次重投仍失败 → 保持 `MANUAL_INTERVENTION` 终态，清单不叠加 |
| `resolvedEntriesCanBePurgedAndReprocessRulesAreEnforced` | 重投成功 → `RESOLVED` 可清理；非法重投返回 `CONFLICT`/`NOT_FOUND` |

## 工程结构

```
src/main/java/com/github/highcumontoa/saga/
├── domain/    # 状态枚举、流程/步骤定义与实例、人工介入清单条目
├── event/     # 事件契约、回执与可区分原因
├── error/     # 归一化错误类别与异常
├── store/     # 进程内实例存储 + 已处理事件表 + 人工介入清单存储
├── core/      # 编排器（状态机、串行化、超时、补偿与重试）+ 人工介入服务
├── api/       # REST 入口与统一错误出口
└── demo/      # 示例流程定义 order-fulfillment
```
