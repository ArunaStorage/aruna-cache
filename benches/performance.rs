use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;

async fn test_cache() {
    todo!()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("performance");
    group.measurement_time(Duration::from_secs(10));
    let runtime = tokio::runtime::Runtime::new().unwrap();

    group.bench_function(BenchmarkId::new("performance_graph", "10s"), |b| {
        b.to_async(&runtime).iter(test_cache);
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
