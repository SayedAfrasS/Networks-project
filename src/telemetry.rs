use std::fs::{File, OpenOptions};
use std::io::{self, Write};

use crate::packet::now_us;

pub struct TelemetryLogger {
    file: File,
}

impl TelemetryLogger {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Self { file })
    }

    pub fn log(&mut self, event: &str, details: &str) -> io::Result<()> {
        writeln!(
            self.file,
            "{{\"ts_us\":{},\"event\":\"{}\",\"details\":\"{}\"}}",
            now_us(),
            event.escape_debug(),
            details.escape_debug()
        )?;

        self.file.flush()?;

        Ok(())
    }
}