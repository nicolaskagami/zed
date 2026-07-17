#![cfg(any(target_os = "linux", target_os = "freebsd", target_os = "illumos"))]
mod linux;

pub use linux::current_platform;
