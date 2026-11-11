# 数据依赖图的内容寻址增量重算数据包

本地分区表变换运行库，提供纯变换注册、直接执行、内容指纹，以及基于 DAG 的内容寻址增量重算（计划器、执行器、差异解释、发布器）。数据文件位于samples，字段和边界见docs/domain.md，持久目录接口见docs/storage.md，增量调度语义见docs/flow.md。

## 本地运行

Python 3.11以上，仅依赖标准库，无需真实账号或外部数据服务。

```bash
python -m datapack.cli inspect samples
python -I demo/incremental_demo.py
python -m unittest discover -s tests -v
python -m compileall -q datapack tests demo
```

第一条命令检查已有资料，第二条演示修改分区、只改参数、破坏缓存、中断恢复、竞争发布与旧发布可读性，第三条运行全部读取、内容存储、领域原语与增量调度检查，第四条构建Python字节码。

## 容器

```bash
docker build -t local-datapack .
docker run --rm --network none local-datapack
docker run --rm --network none local-datapack python -m unittest discover -s tests -v
```

应用没有必须常驻的服务，入口和库可在同一容器离线运行。state和output是本地工作目录，已排除出Git。样本文件只读，修订目录使用独立SQLite文件，旧版本可继续查询。

## 目录

datapack/storage.py提供摘要、原子文件与修订目录；datapack/records.py提供文件读取及重复主键分类；datapack/domain.py提供当前领域原语；datapack/flow/提供图校验、内容身份、计划快照、缓存、运行台账、执行器与发布器；tests验证这些行为。数据加工结果不由资料检查命令自动生成。
