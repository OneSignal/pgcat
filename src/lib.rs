use std::{env, net::Ipv4Addr};

use once_cell::sync::Lazy;

pub mod admin;
pub mod auth_passthrough;
pub mod ban_service;
pub mod client;
pub mod cmd_args;
pub mod config;
pub mod constants;
pub mod dns_cache;
pub mod errors;
pub mod logger;
pub mod messages;
pub mod mirrors;
pub mod plugins;
pub mod pool;
pub mod prometheus;
pub mod query_router;
pub mod scram;
pub mod server;
pub mod sharding;
pub mod stats;
pub mod tls;

/// Store current pgcat IP if PGCAT_IP environment variable is set.
pub static PGCAT_IP: Lazy<Option<Ipv4Addr>> = Lazy::new(|| {
    env::var("PGCAT_IP")
        .ok()
        .and_then(|ip_str| ip_str.parse().ok())
});

fn ipv4_to_i32(ip: Ipv4Addr) -> i32 {
    let octets = ip.octets();
    i32::from_be_bytes(octets)
}

fn i32_to_ipv4(n: i32) -> Ipv4Addr {
    let octets = n.to_be_bytes();
    Ipv4Addr::from(octets)
}

/// Format chrono::Duration to be more human-friendly.
///
/// # Arguments
///
/// * `duration` - A duration of time
pub fn format_duration(duration: &chrono::Duration) -> String {
    let milliseconds = format!("{:0>3}", duration.num_milliseconds() % 1000);

    let seconds = format!("{:0>2}", duration.num_seconds() % 60);

    let minutes = format!("{:0>2}", duration.num_minutes() % 60);

    let hours = format!("{:0>2}", duration.num_hours() % 24);

    let days = duration.num_days().to_string();

    format!(
        "{}d {}:{}:{}.{}",
        days, hours, minutes, seconds, milliseconds
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipv4_i32_conversion() {
        let ip = Ipv4Addr::new(192, 168, 1, 1);
        let n = ipv4_to_i32(ip);
        let ip_converted = i32_to_ipv4(n);
        assert_eq!(ip, ip_converted);
    }
}
