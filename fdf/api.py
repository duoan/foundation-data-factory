from __future__ import annotations

from pathlib import Path

import datasets as hfds

from fdf.config.schema import OutputConfig, PipelineConfig
from fdf.runtime.executor import run_stage


def run_pipeline(
    config: PipelineConfig,
    *,
    ray_address: str | None = None,
) -> hfds.Dataset:
    """Execute a pipeline from configuration.

    This function orchestrates stage execution:
    - Each stage handles its own input/output (reads from stage.input or previous stage's output path)
    - Stages are executed sequentially
    - Each stage writes its output if configured (stage.output)

    Args:
        config: Pipeline configuration
        ray_address: Optional Ray cluster address for distributed execution

    Returns:
        Final output dataset (converted from Daft DataFrame for API compatibility)
    """
    if not config.stages:
        msg = "Pipeline has no stages"
        raise ValueError(msg)

    # Validate first stage has input
    first_stage = config.stages[0]
    if first_stage.input is None:
        msg = f"First stage '{first_stage.name}' must have input configuration"
        raise ValueError(msg)

    # Execute all stages sequentially
    # Each stage handles its own I/O: read input, apply operators, write output
    # Each stage's output automatically becomes the next stage's input
    previous_stage_output: OutputConfig | None = None

    for stage in config.stages:
        # Auto-configure input from previous stage's output if not specified
        if previous_stage_output is not None and stage.input is None:
            # Copy previous stage's output config as this stage's input
            from fdf.config.schema import InputConfig

            stage.input = InputConfig(source=previous_stage_output.source)

        # Stage will read from stage.input (auto-configured from previous stage if needed)
        # Stage will write to stage.output (always required - every stage materializes)
        run_stage(
            stage,
            ray_address=ray_address,
        )
        # Update previous_stage_output for next stage
        previous_stage_output = stage.output

    # Read final output from last stage's output path
    last_stage = config.stages[-1]
    from fdf.io.reader import read_data_source

    final_df = read_data_source(last_stage.output.source).collect()
    return hfds.Dataset.from_dict(final_df.to_pydict())


# Backward compatibility: accept pipeline_yaml path
def run_pipeline_from_yaml(
    pipeline_yaml: str | Path,
    *,
    ray_address: str | None = None,
) -> hfds.Dataset:
    """Run pipeline from YAML file (backward compatibility).

    Args:
        pipeline_yaml: Path to pipeline YAML file
        ray_address: Optional Ray cluster address

    Returns:
        Final output dataset
    """
    config = PipelineConfig.from_yaml_file(str(pipeline_yaml))
    return run_pipeline(config, ray_address=ray_address)
