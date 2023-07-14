use std::time::Duration;

use aruna_cache::cache::Cache;
use aruna_cache::structs::Resource::*;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use diesel_ulid::DieselUlid;

async fn test_cache() {
    // 125k benchmark
    let new_cache = Cache::new();
    let mut project = Project(DieselUlid::generate());
    let mut object = Object(DieselUlid::generate());
    for _ in 0..5 {
        project = Project(DieselUlid::generate());
        for _ in 0..50 {
            let collection = Collection(DieselUlid::generate());
            new_cache
                .add_link(project.clone(), collection.clone())
                .unwrap();
            for _ in 0..50 {
                let dataset = Dataset(DieselUlid::generate());
                new_cache
                    .add_link(collection.clone(), dataset.clone())
                    .unwrap();

                for _ in 0..100 {
                    object = Object(DieselUlid::generate());
                    new_cache.add_link(dataset.clone(), object.clone()).unwrap();
                }
            }
        }
    }
    new_cache.traverse_graph(project).unwrap();
    new_cache.get_parents(object).unwrap();
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
