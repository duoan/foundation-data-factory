# FoundationDataFactory - Rust Implementation

这是一个高性能的 Rust 实现，参考了 [datamap-rs](https://github.com/allenai/datamap-rs) 的设计。

## 特性

- **纯 Rust 实现**：高性能、内存安全
- **Polars 数据引擎**：列式处理，自动并行化
- **保持 YAML 配置格式**：与 Python 版本完全兼容
- **多线程处理**：使用 Rayon 进行并行处理
- **流式处理**：支持大规模数据集

## 安装 Rust

如果还没有安装 Rust：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 构建

```bash
cargo build --release
```

二进制文件会在 `target/release/fdf`

## 运行

```bash
# 运行 pipeline
./target/release/fdf run example_pipeline_with_filter.yaml

# 验证配置
./target/release/fdf validate example_pipeline_with_filter.yaml

# 查看版本
./target/release/fdf version
```

## 项目结构

```
src/
  main.rs          # CLI 入口
  config/          # YAML 配置解析
  operators/       # 操作符实现
    mod.rs
    textstat.rs    # textstat 操作符
  io/              # 数据读取/写入
  runtime/         # Pipeline 执行引擎
```

## 性能优势

相比 Python 版本：

1. **编译优化**：Rust 的零成本抽象和 LLVM 优化
2. **内存效率**：无 GC，精确的内存控制
3. **并行处理**：Polars + Rayon 自动并行化
4. **无 Python 开销**：直接操作数据，无需 Python 对象转换

## 当前实现状态

- ✅ YAML 配置解析
- ✅ Pipeline 执行框架
- ✅ Parquet 读取/写入
- ✅ TextStat 操作符（基础实现）
- ✅ Filter 操作符
- ⏳ Hugging Face 数据集支持（需要完善）
- ⏳ 完整的 textstat 实现（当前是简化版）

## 下一步

1. 完善 textstat 实现（可以使用 Rust 的 textstat crate）
2. 完善 Hugging Face 数据集支持
3. 添加更多操作符
4. 性能优化和基准测试
