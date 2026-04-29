# Hadoop-recsys

基于 **Docker + HDFS + MapReduce（Streaming）+ Spark** 的离线推荐实验：从**表格化日志**构造隐式评分与 **Item-CF** 候选，再用 **Spark ML**（LR / GBT / RF）打分并与协同过滤融合，输出指标与图表。

支持两种数据源：

| 模式 | 数据 | 命令 |
|------|------|------|
| 默认 | 根目录 **`social_ecommerce_data.csv`** | `bash pipeline/run_all_docker.sh` |
| MovieLens 100K | `u.data` / `u.item` / `u.user`（见 `data/ml100k/README.txt`） | `RECSYS_DATASET=ml100k bash pipeline/run_all_docker.sh` |

MovieLens 模式下会先用 **`pipeline/ml100k_build_csv.py`** 生成与电商表头一致的 **`recsys_ml100k.csv`**（评分 ≥4 为 `label=1`；主类型来自 `u.item` 类型位），再走同一套 MR + Spark。

## 环境要求

- Docker Desktop（支持 `docker compose`）
- 机器内存建议 **≥ 8GB**（Spark driver 默认约 3GB）
- **电商默认**：项目根目录有 **`social_ecommerce_data.csv`**
- **MovieLens**：将官方 100K 解压，使 `u.data`、`u.item`、`u.user` 位于 **`data/ml100k/`**（或设置 **`ML100K_DIR`** 指向解压目录）

### Docker 拉取 `apache/spark` 超时（`DeadlineExceeded` / `context deadline exceeded`）

日志里若出现 **`failed to resolve source metadata for docker.io/apache/spark:3.5.3-java17-python3`**，是访问 **Docker Hub** 超时或网络不稳定，不是项目代码错误。可按顺序尝试：

1. **多试几次**：`cd hadoop-docker && docker compose build spark-runner`
2. **先单独拉镜像**（便于重试、看明确进度）：  
   `docker pull apache/spark:3.5.3-java17-python3`
3. **换网络 / 代理 / VPN**（部分网络对 `registry-1.docker.io` 很慢）
4. **配置镜像加速**：在 Docker Desktop → *Docker Engine* 里为 `registry-mirrors` 增加你环境可用的加速地址（各云厂商文档会提供），保存后 **Restart Docker**，再执行 `docker compose build`
5. **已有镜像时跳过构建**：先 `docker pull apache/spark:3.5.3-java17-python3`（及按需 pull 其它基础镜像），再执行  
   `SKIP_DOCKER_BUILD=1 bash pipeline/run_all_docker.sh`  
   或 `cd hadoop-docker && docker compose up -d` 后手动在容器里跑 MR/Spark。

MovieLens 的 **`recsys_ml100k.csv` 已生成成功** 时，说明数据准备没问题；仅 Spark 镜像未构建完成，修好网络后从 **`cd hadoop-docker && docker compose build && docker compose up -d`** 继续即可，不必重新跑 `ml100k_build_csv`。

## 一键运行

在项目**根目录**执行：

```bash
bash pipeline/run_all_docker.sh
```

MovieLens 100K：

```bash
RECSYS_DATASET=ml100k bash pipeline/run_all_docker.sh
# 或数据在其他路径：
ML100K_DIR=/path/to/ml-100k RECSYS_DATASET=ml100k bash pipeline/run_all_docker.sh
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

### 仅生成 MovieLens 对齐 CSV（本机调试）

```bash
python3 pipeline/ml100k_build_csv.py data/ml100k recsys_ml100k.csv
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
data/ml100k/            # 放置 MovieLens u.data / u.item / u.user（见 README.txt）
output/                 # 运行产物（可提交或本地再生）
social_ecommerce_data.csv
recsys_ml100k.csv       # MovieLens 模式生成（默认 .gitignore）
README.md
```

## 许可证

课程/个人学习项目用途请自行与数据提供方确认使用范围；代码可按需补充 LICENSE。
