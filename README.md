# Hadoop-recsys

基于 **Docker + HDFS + MapReduce（Streaming）+ Spark** 的离线推荐实验：从社交电商日志构造隐式评分与 **Item-CF** 候选，再用 **Spark ML**（LR / GBT / RF）打分并与协同过滤融合，输出指标与图表。

## 环境要求

- Docker Desktop（支持 `docker compose`）
- 机器内存建议 **≥ 8GB**（Spark driver 默认约 3GB）
- 项目根目录需有数据文件 **`social_ecommerce_data.csv`**

## 一键运行

在项目**根目录**执行：

```bash
bash pipeline/run_all_docker.sh
```

脚本会：

1. 构建并启动 `hadoop-docker/docker-compose.yml` 中的栈（Namenode、Datanode、ResourceManager、NodeManager、HistoryServer、Spark 等）
2. 等待 HDFS **退出 safe mode**
3. 在 **namenode** 容器内执行 MapReduce 流水线（`run_mapreduce.sh`）
4. 在 **spark-runner** 容器内执行 `recsys_spark.sh`（特征、训练、融合、评估、作图）

### MapReduce 使用 YARN（可选）

需要截 YARN UI 作业页时使用：

```bash
RECSYS_USE_YARN=1 bash pipeline/run_all_docker.sh
```

YARN Web UI：<http://localhost:8088>  
HDFS Namenode UI：<http://localhost:9870>

### 仅重跑 Spark（HDFS 上已有 MR 产物）

```bash
cd hadoop-docker
docker compose exec -T spark-runner bash /workspace/pipeline/run_spark.sh
```

### 仅从 B2 续跑 MR（前面步骤已完成时）

```bash
cd hadoop-docker
docker compose exec -T -e RECSYS_USE_YARN=0 namenode bash /workspace/pipeline/run_mr_tail.sh
```

## 输出说明

| 路径 | 说明 |
|------|------|
| `output/metrics.txt` | 数据协议、模型与融合、测试指标、冷启动与工程说明、Spark 耗时等 |
| `output/pipeline_mr_timing.txt` | 各 MR 阶段耗时记录 |
| `output/chart_*.png` | α 扫描、CF vs 融合、分层图等 |
| `output/stratified_*.csv` | 按性别、活跃度、主导品类的分桶表 |

## 流水线概要

1. **隐式评分（A1/A2）**：从 CSV 行为字段汇总为 `user#item → score`，写入 HDFS `ratings`
2. **共现与相似度（B1/B2）**：用户级共现（辅以品类 Top1 商品作 proxy，见 `compute_cat_top1.py` + `mr_b1_user_proxy_map.py`）；余弦相似度；Top-N 邻居
3. **CF 候选**：`cf_gen_candidates.py` 生成 `user_id, item_id, cf_score` 上传 HDFS
4. **Spark（`recsys_spark.py`）**：HiveQL 兼容 SQL（`hive_compat.sql`）、用户级划分、负采样、LR/GBT/RF、验证集上选 **α** 融合 CF 与模型概率、多样性截断、AUC/PR、Recall@K、NDCG、bootstrap 等

## 目录结构（节选）

```
hadoop-docker/          # Compose、Hadoop/Spark 镜像与配置
pipeline/               # MR 脚本、Spark 主程序、Shell 入口
output/                 # 运行产物（可提交或本地再生）
social_ecommerce_data.csv
README.md
```

## 许可证

课程/个人学习项目用途请自行与数据提供方确认使用范围；代码可按需补充 LICENSE。
