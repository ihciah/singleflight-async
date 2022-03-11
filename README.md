# Singleflight Async
[![Crates.io][crates-badge]][crates-url]
[![MIT/Apache-2 licensed][license-badge]][license-url]

[crates-badge]: https://img.shields.io/crates/v/singleflight-async.svg
[crates-url]: https://crates.io/crates/singleflight-async
[license-badge]: https://img.shields.io/crates/l/singleflight-async.svg
[license-url]: LICENSE-MIT

[Singleflight](https://github.com/gsquire/singleflight) in async style.

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

    for fut in futures.into_iter() {
        assert_eq!(fut.await, "my-result");
        println!("task finished");
    }
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
```