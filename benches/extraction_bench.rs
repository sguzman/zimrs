use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use zimrs::config::ExtractionConfig;
use zimrs::extractor::extract_from_html;

fn extraction_benchmark(c: &mut Criterion) {
    let html = include_str!("data/sample_wiktionary_fragment.html");

    let mut group = c.benchmark_group("extract_from_html");
    for scale in [1_usize, 5, 20] {
        let input = html.repeat(scale);

        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{scale}x")),
            &input,
            |b, payload| {
                let cfg = ExtractionConfig::default();
                b.iter(|| {
                    let extracted = extract_from_html("benchmark", payload, &cfg);
                    black_box(extracted.definitions.len());
                    black_box(extracted.relations.len());
                    black_box(extracted.aliases.len());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, extraction_benchmark);
criterion_main!(benches);
