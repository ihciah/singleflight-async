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
