//! Producer and consumer over tachyon, written as one binary that forks a
//! child so you can see both halves.
//!
//! Run:
//!
//! ```text
//! cd bindings/rust/tachyon
//! cargo build --release
//! cargo run --release --example main      # if placed in examples/
//! ```
//!
//! The child attaches to the region by name. Nothing but the name crosses the
//! process boundary.

use std::process::{exit, Command};
use std::thread;
use std::time::{Duration, Instant};

use tachyon::{Error, Region, RegionConf, Topic, TopicConf};

const REGION: &str = "rust_example";
const TOPIC: &str = "orders";
const COUNT: u64 = 20_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let child = std::env::args().nth(1).unwrap_or_default() == "child";
    if child {
        run_child()
    } else {
        run_parent()
    }
}

fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    let _ = Region::remove(REGION);

    let reg = Region::open(
        REGION,
        RegionConf {
            size: 64 << 20,
            create: true,
            ..Default::default()
        },
    )?;

    let topic = reg.topic(
        TOPIC,
        TopicConf {
            partitions: 4,
            capacity: 1024,
            pool_bytes: 16 << 20,
            ..Default::default()
        },
    )?;

    println!(
        "topic: {} partitions, {} slots each",
        topic.partitions(),
        topic.capacity()
    );

    // --- zero-copy: write the record straight into shared memory ----------
    let mut loan = topic.loan(16)?;
    loan.as_mut_slice()[0..8].copy_from_slice(&20261001u64.to_le_bytes());
    loan.as_mut_slice()[8..16].copy_from_slice(&9911u64.to_le_bytes());
    loan.publish(16, 9911)?;
    println!("published order 20261001 to partition {}", topic.route(9911));

    // --- start the consumer ----------------------------------------------
    let exe = std::env::current_exe()?;
    let mut child = Command::new(exe).arg("child").spawn()?;
    thread::sleep(Duration::from_millis(200));

    // --- bulk send --------------------------------------------------------
    let payload = vec![b'x'; 256];
    let start = Instant::now();
    for i in 0..COUNT {
        loop {
            match topic.send(&payload, i) {
                Ok(()) => break,
                // Ring is full: this is the backpressure signal. Real code
                // picks a policy here (block, drop, spill) instead of looping.
                Err(Error::Full) | Err(Error::OutOfMemory) => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(e) => return Err(Box::new(e)),
            }
        }
    }
    let elapsed = start.elapsed();
    println!(
        "sent {} messages in {:?} ({:.0} msg/s)",
        COUNT,
        elapsed,
        COUNT as f64 / elapsed.as_secs_f64()
    );

    let status = child.wait()?;
    if !status.success() {
        eprintln!("child exited with {}", status);
    }

    let stats = topic.stats()?;
    println!(
        "stats: published={} consumed={} dropped={} reclaimed={}",
        stats.published(),
        stats.consumed(),
        stats.dropped(),
        stats.reclaimed()
    );

    Region::remove(REGION)?;
    Ok(())
}

fn run_child() -> Result<(), Box<dyn std::error::Error>> {
    let reg = Region::open(
        REGION,
        RegionConf {
            create: false,
            ..Default::default()
        },
    )?;
    let topic: Topic = reg.topic(TOPIC, TopicConf::default())?;

    let mut received = 0u64;
    let deadline = Instant::now() + Duration::from_secs(20);
    let want = COUNT + 1; // +1 for the zero-copy order

    while received < want {
        if Instant::now() > deadline {
            return Err(format!("timed out after {received} messages").into());
        }
        let msg = match topic.receive_any(1_000_000_000) {
            Ok(m) => m,
            Err(Error::Empty) | Err(Error::Timeout) => continue,
            Err(e) => return Err(Box::new(e)),
        };
        if received == 0 {
            let order = u64::from_le_bytes(msg[0..8].try_into()?);
            let user = u64::from_le_bytes(msg[8..16].try_into()?);
            println!("child: first message order={order} user={user} partition={}", msg.partition());
        }
        received += 1;
        msg.release();
    }

    println!("child: received {received} messages");
    exit(0);
}
