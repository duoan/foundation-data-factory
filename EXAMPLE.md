# Example Pipeline

这是一个完整的 FDF pipeline 示例，展示如何使用 CLI 运行数据处理流程，使用真实的 Hugging Face 数据集。

## 快速开始

### 1. 准备输入数据（从 Hugging Face 下载）

```bash
# 下载 Hugging Face IMDb 数据集并保存为本地 parquet
uv run python -c "
import datasets as hfds
import pyarrow.parquet as pq
from pathlib import Path

# 下载 IMDb 数据集的前 1000 条数据
print('Downloading IMDb dataset from Hugging Face...')
ds = hfds.load_dataset('imdb', split='train[:1000]')
print(f'Loaded {len(ds)} rows')
print(f'Columns: {ds.column_names}')

# 保存为 parquet
output_dir = Path('example_data/hf_input')
output_dir.mkdir(parents=True, exist_ok=True)
pq.write_table(ds.data.table, output_dir / 'data.parquet')
print(f'Saved to {output_dir / \"data.parquet\"}')
print('Sample:', ds[0])
"
```

### 2. 运行 Pipeline

Pipeline 会自动从 Hugging Face 下载数据（首次运行会下载，后续会使用缓存）：

```bash
uv run fdf run example_pipeline.yaml
```

### 4. 查看输出结果

```bash
# 查看输出文件
ls -la example_data/output/

# 查看 manifest
cat example_data/output/manifest.json

# 读取输出数据
uv run python -c "
import pyarrow.parquet as pq
table = pq.read_table('example_data/output/part-00000.parquet')
print('Output rows:', table.num_rows)
print('Data:', table.to_pydict())
"
```

## Pipeline 配置说明

`example_pipeline.yaml` 包含两个 stage：

1. **Stage 1 (`process_data`)**:
   - **直接从 Hugging Face 读取 IMDb 数据集**（`type: "huggingface"`, `path: "imdb"`）
   - 应用 `passthrough-refiner` operator（不做任何修改）
   - 输出到 `./example_data/stage1_output`

2. **Stage 2 (`process_again`)**:
   - 自动从 Stage 1 的输出读取数据（无需显式配置 input）
   - 再次应用 `passthrough-refiner` operator
   - 输出到 `./example_data/output`

## 使用其他 Hugging Face 数据集

你可以直接在 YAML 配置中使用任何 Hugging Face 数据集，只需修改 `path` 字段：

```yaml
input:
  source:
    type: "huggingface"
    path: "sst2"  # Stanford Sentiment Treebank v2
    streaming: false

# 或者使用带用户名的数据集：
input:
  source:
    type: "huggingface"
    path: "tiiuae/falcon-refinedweb"  # 带用户名的数据集
    streaming: false

# 对于私有数据集，可以添加 token：
input:
  source:
    type: "huggingface"
    path: "your-org/private-dataset"
    token: "${HF_TOKEN}"  # 从环境变量读取
    streaming: false
```

**注意**：首次运行会从 Hugging Face 下载数据，后续运行会使用本地缓存，速度更快。

## 使用 Ray 运行（可选）

如果需要使用 Ray 进行分布式执行：

```bash
uv run fdf run example_pipeline.yaml --ray-address ray://127.0.0.1:10001
```

## 输出结构

每个 stage 的输出目录包含：

- `part-*.parquet`: 数据分区文件（Parquet 格式）
- `manifest.json`: Stage 元数据，包含：
  - `stage`: Stage 名称
  - `output_rows`: 输出行数
  - `path`: 输出路径
