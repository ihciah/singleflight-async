# Singleflight Async
[![Crates.io][crates-badge]][crates-url]
[![MIT/Apache-2 licensed][license-badge]][license-url]

[crates-badge]: https://img.shields.io/crates/v/singleflight-async.svg
[crates-url]: https://crates.io/crates/singleflight-async
[license-badge]: https://img.shields.io/crates/l/singleflight-async.svg
[license-url]: LICENSE-MIT

[Singleflight](https://github.com/gsquire/singleflight) in async style.

## Key Features
- Execute an async task only once for the same key at the same time.
- Cancel safe when the task is dropped.
- Not requires the future to be `Send`/`Sync`, or `'static`.
- Works for all kind of runtimes including tokio, monoio, or others.

## Example
```rust
use singleflight_async::SingleFlight;

#[tokio::main]
async fn main() {
    let group = SingleFlight::new();
    let mut futures = Vec::new();
    for _ in 0..10 {
        futures.push(group.work("key", || async {
            println!("will sleep to simulate async task");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            println!("real task done");
            "my-result"
        }));
    }

    let begin = std::time::Instant::now();
    for fut in futures.into_iter() {
        assert_eq!(fut.await, "my-result");
        println!("task finished");
    }
    println!("time elapsed: {:?}", begin.elapsed());
}
```

The output will be like:
```
will sleep to simulate async task
real task done
task finished
task finished
task finished
task finished
task finished
task finished
task finished
task finished
task finished
task finished
time elapsed: 100.901321ms
```