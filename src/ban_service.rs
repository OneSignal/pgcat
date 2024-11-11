use crate::stats::ClientStats;
use chrono::naive::NaiveDateTime;
use log::{debug, error, warn};
use parking_lot::RwLock;
use std::sync::Arc;

use std::collections::HashMap;

use crate::config::{Address, Role};
pub type BanList = Arc<RwLock<Vec<HashMap<Address, (BanReason, NaiveDateTime)>>>>;
#[derive(Debug, Clone, Default)]
pub struct BanService {
    /// List of banned addresses (see above)
    /// that should not be queried.
    banlist: BanList,

    /// Whether or not we should use primary when replicas are unavailable
    pub replica_to_primary_failover_enabled: bool,

    /// Ban time (in seconds)
    pub ban_time: i64,
}

// Reasons for banning a server.
#[derive(Debug, PartialEq, Clone)]
pub enum BanReason {
    FailedHealthCheck,
    MessageSendFailed,
    MessageReceiveFailed,
    FailedCheckout,
    StatementTimeout,
    AdminBan(i64),
}

pub enum UnbanReason {
    AllReplicasBanned,
    BanTimeExceeded,
    PrimaryBanned,
    NotBanned,
}

impl BanService {
    pub fn new(replica_to_primary_failover_enabled: bool, ban_time: i64) -> Self {
        BanService {
            banlist: Arc::new(RwLock::new(vec![HashMap::new()])),
            replica_to_primary_failover_enabled,
            ban_time,
        }
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. Existing transactions on that replica
    /// will finish successfully or error out to the clients.
    pub fn ban(&self, address: &Address, reason: BanReason, client_info: Option<&ClientStats>) {
        // Count the number of errors since the last successful checkout
        // This is used to determine if the shard is down
        match reason {
            BanReason::FailedHealthCheck
            | BanReason::FailedCheckout
            | BanReason::MessageSendFailed
            | BanReason::MessageReceiveFailed => {
                address.increment_error_count();
            }
            _ => (),
        };

        // Primary can never be banned
        if address.role == Role::Primary {
            return;
        }

        let now = chrono::offset::Utc::now().naive_utc();
        error!("Banning instance {:?}, reason: {:?}", address, reason);
        let mut guard = self.banlist.write();

        if let Some(client_info) = client_info {
            client_info.ban_error();
            address.stats.error();
        }

        guard[address.shard].insert(address.clone(), (reason, now));
    }

    /// Clear the replica to receive traffic again. Takes effect immediately
    /// for all new transactions.
    pub fn unban(&self, address: &Address) {
        warn!("Unbanning {:?}", address);
        let mut guard = self.banlist.write();
        guard[address.shard].remove(address);
    }

    /// Check if address is banned
    /// true if banned, false otherwise
    pub fn is_banned(&self, address: &Address) -> bool {
        let guard = self.banlist.read();

        match guard[address.shard].get(address) {
            Some(_) => true,
            None => {
                debug!("{:?} is ok", address);
                false
            }
        }
    }

    /// Returns a list of banned replicas
    pub fn get_bans(&self) -> Vec<(Address, (BanReason, NaiveDateTime))> {
        let mut bans: Vec<(Address, (BanReason, NaiveDateTime))> = Vec::new();
        let guard = self.banlist.read();
        for banlist in guard.iter() {
            for (address, (reason, timestamp)) in banlist.iter() {
                bans.push((address.clone(), (reason.clone(), *timestamp)));
            }
        }
        bans
    }

    /// Unban all replicas in the shard
    /// This is typically used when all replicas are banned and
    /// we don't allow sending traffic to primary.
    pub fn unban_all_replicas(&self, address: &Address) {
        let mut write_guard = self.banlist.write();
        warn!("Unbanning all replicas.");
        write_guard[address.shard].clear();
    }

    /// Determines whether a replica should be unban and returns the reason
    /// why it should be unbanned.
    ///
    /// UnbanReason:
    /// - All replicas are banned (AllReplicasBanned)
    /// - Ban time is exceeded (BanTimeExceeded)
    /// - Primary is banned (PrimaryBanned, this should never happen)
    /// - Not banned (NotBanned, the replica was unbanned while checking the conditions)
    ///
    /// Returns:
    /// - Some(UnbanReason), if the replica should be unbanned
    /// - None, if the replica should not be unbanned
    pub fn should_unban(
        &self,
        pool_addresses: &[Vec<Address>],
        address: &Address,
    ) -> Option<UnbanReason> {
        // If somehow primary ends up being banned we should return true here
        if address.role == Role::Primary {
            return Some(UnbanReason::PrimaryBanned);
        }

        // If we have replica to primary failover we should not unban replicas
        // as we still have the primary to server traffic.
        if !self.replica_to_primary_failover_enabled {
            // Check if all replicas are banned, in that case unban all of them
            let replicas_available = pool_addresses[address.shard]
                .iter()
                .filter(|addr| addr.role == Role::Replica)
                .count();

            debug!("Available targets: {}", replicas_available);

            let read_guard = self.banlist.read();
            let all_replicas_banned = read_guard[address.shard].len() == replicas_available;
            drop(read_guard);

            if all_replicas_banned {
                return Some(UnbanReason::AllReplicasBanned);
            }
        }

        // Check if ban time is expired
        let read_guard = self.banlist.read();
        let exceeded_ban_time = match read_guard[address.shard].get(address) {
            Some((ban_reason, timestamp)) => {
                let now = chrono::offset::Utc::now().naive_utc();
                match ban_reason {
                    BanReason::AdminBan(duration) => {
                        now.timestamp() - timestamp.timestamp() > *duration
                    }
                    _ => now.timestamp() - timestamp.timestamp() > self.ban_time,
                }
            }
            None => return Some(UnbanReason::NotBanned),
        };
        drop(read_guard);

        if exceeded_ban_time {
            Some(UnbanReason::BanTimeExceeded)
        } else {
            debug!("{:?} is banned", address);
            None
        }
    }
}
