use criterion::{Criterion, criterion_group, criterion_main};

fn protocol_placeholder(criterion: &mut Criterion) {
    criterion.bench_function("protocol_placeholder", |bencher| bencher.iter(|| 1_u64));
}

criterion_group!(benches, protocol_placeholder);
criterion_main!(benches);
