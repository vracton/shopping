use std::time::Instant;

pub struct Stages {
    previous: Instant,
    enabled: bool,
}

impl Stages {
    pub fn new() -> Self {
        Self {
            previous: Instant::now(),
            enabled: std::env::var_os("BENCH_STAGES").is_some_and(|value| value == "1"),
        }
    }

    pub fn mark(&mut self, name: &str) {
        if self.enabled {
            let now = Instant::now();
            eprintln!("stage {name} {:.6}", (now - self.previous).as_secs_f64());
            self.previous = now;
        }
    }
}
