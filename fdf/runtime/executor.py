from __future__ import annotations

import json
from pathlib import Path

import daft

from fdf.config.schema import StageConfig
from fdf.operators.registry import get_operator_class


def _apply_operators_and_write(
    df: daft.DataFrame,
    stage: StageConfig,
) -> None:
    """Apply operators and write output using Daft's distributed execution.

    Operators are applied in-place to partitions during iteration.
    Each partition is processed and written directly - no collection to driver.

    Args:
        df: Input Daft DataFrame (lazy)
        stage: Stage configuration
    """
    import pyarrow.parquet as pq

    # Prepare output directory
    output_path = Path(stage.output.source.path)  # type: ignore[arg-type]
    output_path.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    partition_index = 0

    # Iterate partitions - Daft handles distributed execution
    # Each partition is materialized on its worker, not on driver
    for partition in df.iter_partitions():
        # Apply each operator in-place to this partition (MicroPartition)
        for op_cfg in stage.operators:
            op_name = getattr(op_cfg, "op", None)
            if not op_name:
                continue

            op_cls = get_operator_class(op_name)
            op_instance = op_cls()

            # Get operator parameters from config
            op_params = getattr(op_cfg, "params", None) or {}

            # Apply operator in-place (modifies partition directly)
            op_instance.apply(partition, params=op_params)  # type: ignore[arg-type]

        # Convert to Arrow Table for writing
        partition_table = partition.to_arrow()  # type: ignore[possibly-missing-attribute]
        total_rows += partition_table.num_rows

        # Write partition directly to disk - no collection to driver
        # Use PyArrow to write individual partition
        partition_file = output_path / f"part-{partition_index:05d}.parquet"
        pq.write_table(partition_table, partition_file)
        partition_index += 1

    # Write manifest if output is parquet
    if stage.output.source.type == "parquet":
        manifest_path = output_path / "manifest.json"

        manifest = {
            "stage": stage.name,
            "output_rows": total_rows,
            "path": str(output_path),
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))


def run_stage(
    stage: StageConfig,
    *,
    input_df: daft.DataFrame | None = None,
    ray_address: str | None = None,
) -> None:
    """Run a stage using Daft for execution.

    A stage performs three main operations:
    1. Read input data (from stage.input or input_df)
    2. Apply operators sequentially
    3. Write output data (to stage.output - always required)

    Data is always materialized to disk. Next stage reads from previous stage's output path.

    If `ray_address` is provided, Daft will execute on Ray cluster.
    Otherwise, execution happens locally.

    Args:
        stage: Stage configuration
        input_df: Optional input Daft DataFrame (for programmatic usage)
        ray_address: Optional Ray cluster address (e.g., "ray://127.0.0.1:10001").
                     If provided, Daft will execute on Ray. Otherwise, local execution.
    """
    # Set Daft runner if Ray address is provided
    # Use noop_if_initialized=True to avoid errors if runner is already set
    if ray_address is not None:
        daft.set_runner_ray(ray_address, noop_if_initialized=True)
    else:
        # Check if Ray is available and initialized, if so use it
        try:
            import ray

            if ray.is_initialized():  # type: ignore[misc]
                # Set Ray runner, but don't fail if already set
                daft.set_runner_ray(noop_if_initialized=True)
        except ImportError:
            # Ray not available, use local execution
            pass

    # Step 1: Read input data
    # Priority: input_df > stage.input
    if input_df is not None:
        df = input_df
    elif stage.input is not None:
        from fdf.io.reader import read_input

        df = read_input(stage.input)
    else:
        msg = "Stage must have input configuration or receive input_df parameter"
        raise ValueError(msg)

    # Apply operators and write output
    # Operators are applied during the write phase to keep data distributed
    # Daft's write operations handle distributed execution automatically
    _apply_operators_and_write(df, stage)


# Backward compatibility aliases
def run_stage_local(
    stage: StageConfig,
    *,
    input_df: daft.DataFrame | None = None,
) -> None:
    """Run a stage locally (backward compatibility alias).

    Use `run_stage()` instead.
    """
    run_stage(
        stage,
        input_df=input_df,
        ray_address=None,
    )


def run_stage_ray(
    stage: StageConfig,
    *,
    input_df: daft.DataFrame | None = None,
    ray_address: str | None = None,
) -> None:
    """Run a stage using Ray for distributed execution (backward compatibility).

    This is a wrapper around `run_stage()` that sets the Ray runner.
    For new code, use `run_stage()` directly with `ray_address` parameter.

    Args:
        stage: Stage configuration
        input_df: Optional input Daft DataFrame (for programmatic usage)
        ray_address: Ray cluster address (e.g., "ray://127.0.0.1:10001").
                     If None, will try to use existing Ray cluster.
    """
    # If ray_address not provided, try to detect existing Ray cluster
    if ray_address is None:
        try:
            import ray

            if ray.is_initialized():  # type: ignore[misc]
                # Use existing Ray cluster - Daft will auto-detect
                ray_address = None  # Let Daft auto-detect
            else:
                # Initialize local Ray cluster
                ray.init(ignore_reinit_error=True)  # type: ignore[misc]
                # Daft will auto-detect the local Ray cluster
                ray_address = None
        except ImportError as err:
            msg = "Ray is required for ray_runtime. Install with: pip install ray"
            raise ImportError(msg) from err

    run_stage(
        stage,
        input_df=input_df,
        ray_address=ray_address,
    )
