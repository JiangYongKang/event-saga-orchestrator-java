# Event Saga Orchestrator (Java / Spring Boot)

单进程内可复现的**事件驱动流程编排 + 补偿（Saga）**骨架。不依赖任何外部消息中间件或数据库：
事件存内存只追加日志（Event Store），流程状态存内存仓储，异常事件进内存死信队列。

适用于演示与验证以下能力：

- 事件驱动的流程**创建、推进、状态查询**；
- **至少一次投递**下，按 `eventId` 精确去重，重复事件不产生第二次副作用，且拒绝原因可区分；
- **乱序 / 并发**推进：同一 `businessKey` 分段加锁串行化 + `expectedVersion` 乐观并发检测；
- 步骤失败后的**反向补偿**、补偿**有界重试**、重试耗尽进入**人工介入终态**并落**死信**，绝不静默丢弃；
- 超时 / 重试耗尽 / 死信 / 冲突等异常的**归一化错误码**，对外不泄露内部异常堆栈；
- 完整日志：事件流水 `[EVENT-RECV]`、状态迁移依据 `[STATE-MOVE]`、副作用 `[SIDE-EFFECT:*]`、重试 `[RETRY]`、死信 `[DEAD-LETTER]`。

---

## 1. 快速开始

要求 JDK 21+、Maven 3.6+（离线可用，依赖已缓存时加 `-o`）。

```bash
# 运行全部测试（15 个用例，零退避、毫秒级）
mvn test

# 启动应用（默认随机端口，见 application.properties 的 server.port=0）
mvn spring-boot:run
```

启动后示例流程 `ORDER` 自动注册，包含 4 个步骤：

| stepIndex | 名称 | 含义 |
|-----------|------|------|
| 0 | reserveInventory | 预留库存 |
| 1 | debitPayment | 扣款 |
| 2 | createOrder | 创建订单 |
| 3 | notifyShipping | 通知发货 |

### 1.1 curl 手工验证

```bash
BASE=http://localhost:${PORT}

# 1) 创建流程
curl -s -XPOST $BASE/api/sagas -H 'Content-Type: application/json' \
  -d '{"businessKey":"order-1001","flowType":"ORDER"}'

# 2) 依次推进（eventId 由调用方提供，用于幂等）
for i in 0 1 2 3; do
  curl -s -XPOST $BASE/api/sagas/order-1001/events -H 'Content-Type: application/json' \
    -d "{\"eventId\":\"evt-order-1001-$i\",\"stepIndex\":$i}"
  echo
done

# 3) 查询状态（含每个步骤的状态、正向尝试次数、补偿尝试次数）
curl -s $BASE/api/sagas/order-1001

# 4) 人工触发补偿（也用于 COMPENSATION_FAILED 终态下的人工介入重试）
curl -s -XPOST $BASE/api/sagas/order-1001/compensate -H 'Content-Type: application/json' \
  -d '{"reasonCode":"MANUAL"}'
```

---

## 2. 事件契约（Event Contract）

所有推进都必须由事件驱动，事件为不可变记录 `SagaEvent`：

| 字段 | 说明 |
|------|------|
| `eventId` | 全局唯一，**幂等键**。同一 `eventId` 在全局只允许产生一次副作用 |
| `businessKey` | 业务键，流程实例标识；同一键的所有处理在分段锁内串行 |
| `type` | `SAGA_CREATED` / `STEP_ADVANCE` / `COMPENSATION_TICK` |
| `stepIndex` | 目标步骤序号（严格 0..N-1）；`COMPENSATION_TICK` 传 -1 |
| `expectedVersion` | 可选的乐观版本号；与当前版本不一致返回冲突 |
| `payload` | 合并进 `SagaContext`，在步骤间传递 |
| `occurredAt` | 事件产生时间 |

处理结果 `EventProcessingResult` 字段：

| status | 含义 | HTTP | reasonCode 示例 |
|--------|------|------|-----------------|
| `ACCEPTED` | 正常接受并产生副作用 | 200 | `STEP_DONE` / `ALL_STEPS_DONE` |
| `COMPENSATED` | 前向失败后补偿完成 | 200 | `COMPENSATED_AFTER_FAILURE:FORWARD_RETRY_EXHAUSTED` |
| `DUPLICATE` | 重复事件（至少一次投递），无副作用 | 409 | `DUPLICATE_EVENT`、`DUPLICATE_EVENT:SAGA_ALREADY_EXISTS`、`DUPLICATE_EVENT:ALREADY_COMPENSATED` |
| `STALE` | 过期 / 乱序事件，无副作用 | 409 | `STALE_EVENT:STEP_ALREADY_DONE`、`OUT_OF_ORDER:STEP_AHEAD`、`STALE_EVENT:SAGA_ALREADY_COMPLETED`、`STALE_EVENT:COMPENSATION_FAILED_AWAITING_MANUAL_INTERVENTION` |
| `CONFLICT` | `expectedVersion` 乐观冲突，无副作用 | 409 | `VERSION_CONFLICT` |
| `DEAD_LETTER` | 补偿重试耗尽，已落死信等待人工介入 | 422 | `COMPENSATION_RETRY_EXHAUSTED` |

> 重复 / 乱序 / 冲突是“被识别的正常拒绝”，返回结构化结果而非异常堆栈；
> 真正的错误走 `ErrorResponse{errorCode,message,...}`（见下）。

### 归一化错误码（对外）

`VALIDATION_ERROR(400)`、`NOT_FOUND(404)`、`DUPLICATE_EVENT(409)`、`STALE_EVENT(409)`、
`CONCURRENT_CONFLICT(409)`、`STEP_TIMEOUT(504)`、`RETRY_EXHAUSTED(422)`、
`COMPENSATION_FAILED(422)`、`DEAD_LETTER(422)`、`INTERNAL_ERROR(500)`。

- 下游步骤返回 `STEP_TIMEOUT` 原因时归类为超时；
- 重试达到上限统一归类为 `*_RETRY_EXHAUSTED`；原始原因仅保留在服务端日志；
- 步骤抛出的任何 `RuntimeException` 都被捕获并归一化，**不向调用方泄露类名/堆栈**。

---

## 3. 状态机（State Machine）

```text
                 createSaga
  (不存在) ───────────────────────► RUNNING ──最后一步成功──► COMPLETED
                                     │  ▲                        │
                              步骤成功  │  │下一步                  │ 过期事件
                                     ▼  │                        ▼
                                  (推进中)                  STALE 拒绝
                                     │
                          步骤重试仍失败
                                     ▼
                                COMPENSATING ──全部补偿成功──► COMPENSATED
                                     │
                          某步补偿重试耗尽
                                     ▼
                          COMPENSATION_FAILED ◄── 明确人工介入终态
                                     │
                    人工修复后 POST /compensate
                                     └──成功──► COMPENSATED
```

不变式：

1. 每次真实状态迁移 `version + 1`；拒绝（重复/乱序/冲突）与同态保存**不升版本**，版本不会回退。
2. 终态 `COMPLETED / COMPENSATED / COMPENSATION_FAILED` 下到达的 `STEP_ADVANCE` 一律 `STALE` 拒绝。
3. `COMPENSATION_FAILED` 是终态而非中间悬挂态：必须通过 `/compensate` 人工介入才能恢复；恢复前任何推进都被拒绝。
4. 事件只有在**实际驱动迁移**时才追加进 Event Store；被拒绝事件不入流，保证事件流即“真实发生过的迁移”。

---

## 4. 补偿规则（Compensation）

1. **触发时机**：某步正向执行在有界重试后仍失败，立即自动进入 `COMPENSATING`；也可由 `POST /{key}/compensate` 人工触发。
2. **回滚顺序**：只回滚状态为 `COMPLETED`（确实产生过副作用）的步骤，严格按执行相反顺序（N-1 → 0）执行。
3. **失败步骤本身不补偿**：前向失败的步骤未完成副作用，不执行其补偿。
4. **有界重试**：每个补偿动作独立按 `RetryPolicy(maxAttempts=3, 线性退避)` 重试；瞬时失败可在重试内恢复。
5. **重试耗尽**：
   - 当前补偿步骤标记 `FAILED` 并打 `compensationPoison` 标记；
   - 流程进入 `COMPENSATION_FAILED` 终态，**立即停止**回滚更早的步骤（避免补偿顺序被破坏）；
   - 生成一条可追溯事件写入**死信队列**（含失败步骤、原因、尝试次数），并打印 `[DEAD-LETTER]` error 日志。
6. **人工介入恢复**：修复下游后再次 `POST /compensate`，编排器从 poison 步骤（中断点）继续补偿，成功后继续反向回滚更早步骤，最终到 `COMPENSATED`；补偿尝试次数跨轮累计，便于审计。
7. **幂等补偿**：`COMPENSATED` 后重复触发补偿返回 `DUPLICATE_EVENT:ALREADY_COMPENSATED`，不产生额外补偿副作用。

---

## 5. 幂等与重试策略（Idempotency & Retry）

- **事件幂等**：Event Store 维护全局 `eventId` 索引 + 按 `businessKey` 分组的只追加事件流。
  处理任何事件前先查 `eventId`，命中则直接返回 `DUPLICATE`，动作代码不会被执行第二次。
- **顺序约束**：`STEP_ADVANCE.stepIndex` 必须严格等于 `currentStepIndex + 1`：
  小于则 `STALE_EVENT:STEP_ALREADY_DONE`，大于则 `OUT_OF_ORDER:STEP_AHEAD`。
- **并发串行化**：`KeyLockManager` 按 `businessKey` 取 `ReentrantLock`，同一流程的创建/推进/补偿全部串行；
  不同 `businessKey` 之间完全并行。锁内再做 `expectedVersion` 乐观检测，提供双重冲突防护。
- **副作用计数**：示例动作 `ControllableStepAction` 用原子计数器记录正向/补偿调用与成功次数，
  测试直接断言“重复/乱序/冲突事件下成功副作用次数恒为 1”。
- **重试退避**：正向与补偿均为“最多 N 次 + 初始退避 × 尝试次数”的线性退避；
  生产默认 10ms 起步，单元测试以 `Duration.ZERO` 注入。

---

## 6. 可观测性：日志中的事件流水与迁移依据

| 日志前缀 | 含义 |
|----------|------|
| `[EVENT-RECV]` | 收到事件及版本信息；重复事件在此打印 `=> DUPLICATE` |
| `[EVENT-REJECT]` | 拒绝原因（DUPLICATE/STALE/CONFLICT）|
| `[STATE-MOVE]` | 状态迁移及依据（reason、version）|
| `[STEP-ATTEMPT]` / `[RETRY]` | 步骤尝试结果与重试过程 |
| `[SIDE-EFFECT:FORWARD]` / `[SIDE-EFFECT:COMPENSATE]` | 真实副作用发生点与调用序号 |
| `[COMPENSATE-OK]` | 单步补偿成功与累计补偿次数 |
| `[DEAD-LETTER]` | 补偿耗尽落死信（error 级），提示 `MANUAL_INTERVENTION_REQUIRED` |
| `[FLOW-REGISTER]` / `[CONFLICT]` / `[API-ERROR]` | 流程注册、版本冲突、对外错误 |

事件流可通过 `SagaOrchestrator.getEventStream(businessKey)` 获取（仅含真实迁移事件）。

---

## 7. 测试覆盖

| 测试类 | 场景 |
|--------|------|
| `SagaOrchestratorTest` | 正常完成；**重复投递**无重复副作用；**乱序**（跳步/过期）拒绝后可继续；`expectedVersion` **并发冲突**；同一目标步骤**并发推进**恰好生效一次 |
| `SagaCompensationTest` | 前向失败**反向补偿**顺序与次数；补偿瞬时失败重试恢复；补偿持续失败进入 `COMPENSATION_FAILED` + 死信且不继续回滚；**人工介入恢复**到 `COMPENSATED` 且尝试次数跨轮累计；异常归一化 |
| `SagaApiIntegrationTest` | REST 端到端 happy path 与 HTTP 重复 409；乱序 409；未知流程 400；补偿耗尽 422 + 终态查询 + 补偿次数断言 |

```bash
mvn test                      # 全部 15 个用例
mvn test -Dtest=SagaOrchestratorTest
mvn test -Dtest=SagaCompensationTest
mvn test -Dtest=SagaApiIntegrationTest
```

---

## 8. 代码结构

```
domain/      SagaState/EventType/StepStatus、SagaInstance、SagaStepRecord、StepAction/StepDefinition、StepResult、SagaContext
event/       SagaEvent（事件契约）、EventAcceptStatus、EventProcessingResult
exception/   ErrorCode（归一化错误码 + HTTP 映射）、SagaException、ErrorResponse
store/       EventStore / SagaRepository 接口，InMemorySagaStore（并发集合 + 全局 eventId 索引）
engine/      SagaOrchestrator（编排核心）、KeyLockManager、RetryPolicy、SagaFlow、SagaFlowRegistry
deadletter/  DeadLetterQueue、DeadLetterEntry
flow/        OrderFlowConfiguration（示例 ORDER 流程）、ControllableStepAction（可注入故障/计数）
api/         SagaController、GlobalExceptionHandler、请求/视图 DTO
```

## 9. 范围与局限

- 存储为内存实现，进程重启数据丢失；接口（`EventStore`/`SagaRepository`）可替换为持久化实现。
- 未引入外部消息代理；“至少一次投递”语义通过调用方重发同一 `eventId` 模拟与验证。
- 超时分类通过动作返回 `STEP_TIMEOUT` 原因模拟，避免真实线程 sleep 引入测试不稳定性；
  生产实现可在 `StepAction` 外层用 `CompletableFuture.orTimeout(...)` 包装，将超时统一映射为 `STEP_TIMEOUT`。
