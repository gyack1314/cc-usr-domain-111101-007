# 内容寻址增量重算

在已有纯变换（datapack/domain.py）与修订目录（datapack/storage.py）之上，datapack/flow 提供计划器、执行器、差异解释和发布器，全部为本地表格运算，仅依赖标准库。

## 身份与失效

- 图声明：节点含 id、transform、inputs、params；目标声明要交付的节点集合。节点输出 id 必须唯一。
- 节点身份（缓存键）= 哈希（节点 id、变换代码摘要、规范化参数摘要、输入引用摘要）。输入引用对外部分区使用其行内容摘要，对上游节点使用其节点身份，形成 Merkle 式传递。
- 变换代码经 `inspect.getsource` 摘要，参数经规范化 JSON 摘要；两者任一变化即换身份。
- 选择器（inputs 列表）本身参与摘要：增删引用、改引用顺序都会换身份，即使行内容恰好相同也必须重算。
- 目录中新增未被任何节点引用的分区不进入计划快照，不使任何节点失效。
- 图版本摘要覆盖整图节点与目标声明，固定在计划上；图被改动后旧计划直接判过期。

## plan 与 run 分离

- `Planner.make_plan` 在执行前一次性校验图：缺边、循环、重复输出、未知分区/目标引用、未登记变换全部收集为 issues，抛 GraphValidationError，不依赖执行顺序偶然暴露。
- 计划持久化图版本、每个被引用分区的内容摘要、每个节点的身份与目标清单。
- 运行先经 SnapshotSource 核验图版本与全部分区摘要，节点读取分区时再次比对；任何不一致抛 PlanStaleError，不会悄悄读到新文件。
- 执行按拓扑序：缓存完整则复用；缓存损坏（摘要不符、结构破坏、截断）移入 quarantine 并重建；节点失败后其下游标记 blocked，阻断节点的旧成功内容不进入本次成功清单。
- Ledger（SQLite）记录每节点最近状态与全部尝试历史；重试同一计划时已成功且缓存完好的节点不重复调用变换，失败节点可再次执行，进程重启后行为一致。
- 变换执行器可注入（Executor 的 transform_executor 参数），调用方可包装它统计真实调用次数。

## 差异解释

`Planner.explain` 逐节点给出 cache_hit / cache_corrupted / new_node / invalidated 及具体原因：变换代码变化、参数变化、选择器变化、上游身份变化、读取分区内容变化。维护人员应先看 diff 再 run。

## 发布

- 只有计划目标的全部传递依赖节点在本计划台账中为 succeeded，且缓存对象仍可通过校验，Publisher 才更新发布指针；否则抛 PublishRejected。
- 发布内容写入 RevisionStore 的 release 流，带预期前版本乐观核对；版本不符抛 Conflict，落败方的缓存与台账结果原样保留，不覆盖胜者。
- 每次发布产生新修订号，`read_release(revision)` 可读取任意旧版本，旧发布一直可读。

## 状态目录

```
<workdir>/cache/objects/<identity>.json   内容寻址缓存
<workdir>/cache/quarantine/               损坏缓存隔离区
<workdir>/plans/<plan_id>.json            固定图版本与输入摘要的计划
<workdir>/plans/latest                    最近计划指针
<workdir>/ledger.sqlite3                  节点状态与尝试历史
<workdir>/revisions.sqlite3               发布修订目录
```

## 命令行

```bash
python -m datapack.cli plan    --workdir state --partitions samples/partitions.json --nodes samples/nodes.json --targets samples/targets.json
python -m datapack.cli diff    --workdir state --partitions samples/partitions.json
python -m datapack.cli run     --workdir state --partitions samples/partitions.json --nodes samples/nodes.json --targets samples/targets.json
python -m datapack.cli state   --workdir state
python -m datapack.cli publish --workdir state --partitions samples/partitions.json --nodes samples/nodes.json --targets samples/targets.json [--expected N]
python -m datapack.cli release --workdir state [--revision N]
```

图校验失败退出码为 3，其它输入/状态错误退出码为 2。

## 演示与测试

```bash
python -I demo/incremental_demo.py
python -m unittest discover -s tests -v
```

演示覆盖：修改单个分区的精准失效、只改参数、缓存损坏重建、节点中断与重启恢复、竞争发布与旧发布可读性、计划过期保护。
