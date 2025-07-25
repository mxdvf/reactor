use criterion::{Criterion, criterion_group, criterion_main};

fn send() {
    // TODO
}

fn bench_send(c: &mut Criterion) {
    // TODO
}

fn recv() {
    // TODO
}

fn bench_recv(c: &mut Criterion) {
    // TODO
}

criterion_group!(benches, bench_send, bench_recv);
criterion_main!(benches);
