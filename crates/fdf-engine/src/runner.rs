use crate::plan::Plan;
use crate::spec::PipelineSpec;
use fdf_sdk::OperatorRegistry;
use fdf_sdk::Result;
use std::time::Instant;

pub fn run_pipeline(spec: PipelineSpec, registry: &OperatorRegistry) -> Result<()> {
    let plan = Plan::compile(spec, registry)?;

    // Start timing
    let start_time = Instant::now();

    // Execute pipeline and get statistics
    let stats = plan.execute()?;

    // Calculate elapsed time
    let elapsed = start_time.elapsed();

    // Print comprehensive statistics
    println!("\n=== Processing Statistics ===");
    println!(
        "Total processing time: {:.2} seconds",
        elapsed.as_secs_f64()
    );
    println!("Number of documents processed: {}", stats.num_documents);

    // Print I/O statistics
    let write_time_percent = if elapsed.as_millis() > 0 {
        (stats.write_time_ms as f64 * 100.0) / elapsed.as_millis() as f64
    } else {
        0.0
    };

    // Calculate estimated read time (total time minus processing and write time)
    let total_processing_time_ms: u64 = stats
        .step_statistics
        .iter()
        .map(|s| s.processing_time_ms)
        .sum();
    let estimated_read_time_ms =
        if elapsed.as_millis() > (total_processing_time_ms + stats.write_time_ms) as u128 {
            elapsed.as_millis() - (total_processing_time_ms + stats.write_time_ms) as u128
        } else {
            0
        };
    let read_time_percent = if elapsed.as_millis() > 0 {
        (estimated_read_time_ms as f64 * 100.0) / elapsed.as_millis() as f64
    } else {
        0.0
    };

    println!("\n--- I/O Statistics ---");
    println!(
        "Read time (estimated): {:.2}ms ({:.2}%)",
        estimated_read_time_ms, read_time_percent
    );
    println!(
        "Write time: {:.2}ms ({:.2}%)",
        stats.write_time_ms, write_time_percent
    );

    if !stats.step_statistics.is_empty() {
        println!("\n--- Pipeline Step Statistics ---");
        for step_stat in &stats.step_statistics {
            let processing_time_percent = if elapsed.as_millis() > 0 {
                (step_stat.processing_time_ms as f64 * 100.0) / elapsed.as_millis() as f64
            } else {
                0.0
            };

            let removed_percent_of_remaining = if step_stat.documents_remaining_before > 0 {
                (step_stat.documents_removed as f64 * 100.0)
                    / step_stat.documents_remaining_before as f64
            } else {
                0.0
            };

            let removed_percent_of_total = if step_stat.total_documents > 0 {
                (step_stat.documents_removed as f64 * 100.0) / step_stat.total_documents as f64
            } else {
                0.0
            };

            println!("Step {} ({})", step_stat.step_index, step_stat.step_name);
            println!(
                "  Processing time: {:.2}ms ({:.2}%)",
                step_stat.processing_time_ms, processing_time_percent
            );
            println!(
                "  Documents removed: {} ({:.2}% of remaining, {:.2}% of total)",
                step_stat.documents_removed, removed_percent_of_remaining, removed_percent_of_total
            );
        }
    }

    println!("============================\n");

    Ok(())
}
