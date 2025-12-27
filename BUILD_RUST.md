# 构建 Rust 版本

## 安装 Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

## 构建项目

```bash
# Debug 版本（开发用）
cargo build

# Release 版本（生产用，优化）
cargo build --release
```

二进制文件位置：
- Debug: `target/debug/fdf`
- Release: `target/release/fdf`

## 运行

```bash
# 运行 pipeline
./target/release/fdf run example_pipeline_with_filter.yaml

# 验证配置
./target/release/fdf validate example_pipeline_with_filter.yaml
```

## 开发

```bash
# 运行测试
cargo test

# 检查代码
cargo check

# 格式化代码
cargo fmt

# Lint
cargo clippy
```

## 性能对比

Rust 版本相比 Python 版本的优势：

1. **编译优化**：LLVM 后端优化，零成本抽象
2. **内存效率**：无 GC，精确内存控制
3. **并行处理**：Polars + Rayon 自动并行化
4. **无 Python 开销**：直接操作数据，无需 Python 对象转换

预期性能提升：**10-100x**（取决于数据量和操作类型）
