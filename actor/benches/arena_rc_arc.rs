use bumpalo::Bump;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
struct MyData {
    a: usize,
    b: usize,
    c: usize,
}

fn bench_arc_alloc(c: &mut Criterion) {
    c.bench_function("Arc allocation", |b| {
        b.iter(|| {
            let data = Arc::new(MyData { a: 1, b: 2, c: 3 });
            black_box(data);
        })
    });
}

fn bench_rc_alloc(c: &mut Criterion) {
    c.bench_function("Rc allocation", |b| {
        b.iter(|| {
            let data = Rc::new(MyData { a: 1, b: 2, c: 3 });
            black_box(data);
        })
    });
}

fn bench_arena_alloc(c: &mut Criterion) {
    let arena = Bump::new();
    c.bench_function("Arena allocation", |b| {
        b.iter(|| {
            let data = arena.alloc(MyData { a: 1, b: 2, c: 3 });
            black_box(data);
        })
    });
}

criterion_group!(benches, bench_arc_alloc, bench_rc_alloc, bench_arena_alloc);
criterion_main!(benches);
