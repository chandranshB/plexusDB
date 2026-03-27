//! SWIM-based gossip protocol for cluster membership and failure detection.
//!
//! SWIM (Scalable Weakly-consistent Infection-style Membership) provides:
//! - O(log n) convergence time for membership changes
//! - Bounded false-positive failure detection
//! - Piggybacked state dissemination on ping/ack messages
//!
//! Protocol:
//! 1. Every `T` seconds, pick a random member and send PING
//! 2. If no ACK within timeout → send PING-REQ to K random members
//! 3. If still no ACK → mark as SUSPECTED
//! 4. After suspicion timeout → mark as DEAD

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

/// Member state in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberState {
    Alive,
    Suspected,
    Dead,
    Left,
}

/// A cluster member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    pub node_id: String,
    pub address: SocketAddr,
    pub state: MemberState,
    pub incarnation: u64,
    pub last_state_change: u64, // unix timestamp millis
    pub metadata: HashMap<String, String>,
}

/// Gossip message types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Ping {
        sender: String,
        incarnation: u64,
        /// Piggybacked membership updates.
        updates: Vec<MemberUpdate>,
    },
    Ack {
        sender: String,
        incarnation: u64,
        updates: Vec<MemberUpdate>,
    },
    PingReq {
        sender: String,
        target: String,
    },
    Join {
        node_id: String,
        address: SocketAddr,
        metadata: HashMap<String, String>,
    },
    Leave {
        node_id: String,
    },
}

/// A membership state change piggybacked on gossip messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberUpdate {
    pub node_id: String,
    pub state: MemberState,
    pub incarnation: u64,
    pub address: Option<SocketAddr>,
}

/// Gossip protocol configuration.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// This node's ID.
    pub node_id: String,
    /// Listen address for gossip UDP.
    pub bind_addr: SocketAddr,
    /// Ping interval.
    pub ping_interval: Duration,
    /// Ping timeout before trying indirect.
    pub ping_timeout: Duration,
    /// Number of indirect ping targets.
    pub indirect_checks: usize,
    /// Suspicion timeout before declaring dead.
    pub suspicion_timeout: Duration,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            bind_addr: "0.0.0.0:7947".parse().unwrap(),
            ping_interval: Duration::from_secs(1),
            ping_timeout: Duration::from_millis(500),
            indirect_checks: 3,
            suspicion_timeout: Duration::from_secs(5),
        }
    }
}

/// The gossip protocol engine.
pub struct GossipEngine {
    config: GossipConfig,
    members: Arc<RwLock<HashMap<String, Member>>>,
    local_incarnation: Arc<RwLock<u64>>,
    /// Pending updates to piggyback on next messages.
    pending_updates: Arc<RwLock<Vec<MemberUpdate>>>,
}

impl GossipEngine {
    /// Create a new gossip engine.
    pub fn new(config: GossipConfig) -> Self {
        let mut members = HashMap::new();

        // Add self
        let self_member = Member {
            node_id: config.node_id.clone(),
            address: config.bind_addr,
            state: MemberState::Alive,
            incarnation: 0,
            last_state_change: now_millis(),
            metadata: HashMap::new(),
        };
        members.insert(config.node_id.clone(), self_member);

        Self {
            config,
            members: Arc::new(RwLock::new(members)),
            local_incarnation: Arc::new(RwLock::new(0)),
            pending_updates: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Add a seed node to bootstrap the cluster.
    pub fn add_seed(&self, node_id: String, address: SocketAddr) {
        let mut members = self.members.write();
        members.insert(
            node_id.clone(),
            Member {
                node_id,
                address,
                state: MemberState::Alive,
                incarnation: 0,
                last_state_change: now_millis(),
                metadata: HashMap::new(),
            },
        );
    }

    /// Get all alive members.
    pub fn alive_members(&self) -> Vec<Member> {
        self.members
            .read()
            .values()
            .filter(|m| m.state == MemberState::Alive)
            .cloned()
            .collect()
    }

    /// Get all members regardless of state.
    pub fn all_members(&self) -> Vec<Member> {
        self.members.read().values().cloned().collect()
    }

    /// Get the number of alive members.
    pub fn alive_count(&self) -> usize {
        self.members
            .read()
            .values()
            .filter(|m| m.state == MemberState::Alive)
            .count()
    }

    /// Process an incoming gossip message.
    pub fn handle_message(&self, msg: GossipMessage) -> Option<GossipMessage> {
        match msg {
            GossipMessage::Ping {
                sender: _sender,
                incarnation: _incarnation,
                updates,
            } => {
                // Apply piggybacked updates
                self.apply_updates(&updates);

                // Respond with ACK
                let our_updates = self.drain_pending_updates();
                Some(GossipMessage::Ack {
                    sender: self.config.node_id.clone(),
                    incarnation: *self.local_incarnation.read(),
                    updates: our_updates,
                })
            }

            GossipMessage::Ack {
                sender,
                incarnation,
                updates,
            } => {
                self.apply_updates(&updates);

                // Mark sender as alive
                let mut members = self.members.write();
                if let Some(member) = members.get_mut(&sender) {
                    if member.state == MemberState::Suspected {
                        member.state = MemberState::Alive;
                        member.incarnation = incarnation;
                        member.last_state_change = now_millis();
                    }
                }

                None
            }

            GossipMessage::Join {
                node_id,
                address,
                metadata,
            } => {
                let mut members = self.members.write();
                members.insert(
                    node_id.clone(),
                    Member {
                        node_id: node_id.clone(),
                        address,
                        state: MemberState::Alive,
                        incarnation: 0,
                        last_state_change: now_millis(),
                        metadata,
                    },
                );

                tracing::info!(node = %node_id, addr = %address, "node joined cluster");

                // Broadcast update
                self.pending_updates.write().push(MemberUpdate {
                    node_id,
                    state: MemberState::Alive,
                    incarnation: 0,
                    address: Some(address),
                });

                None
            }

            GossipMessage::Leave { node_id } => {
                let mut members = self.members.write();
                if let Some(member) = members.get_mut(&node_id) {
                    member.state = MemberState::Left;
                    member.last_state_change = now_millis();
                }
                tracing::info!(node = %node_id, "node left cluster");
                None
            }

            GossipMessage::PingReq {
                sender: _sender,
                target: _target,
            } => {
                // Forward ping to target on behalf of sender
                // In real implementation, we'd send a Ping to target
                // and relay the response back to sender
                None
            }
        }
    }

    /// Apply membership updates received from other nodes.
    fn apply_updates(&self, updates: &[MemberUpdate]) {
        let mut members = self.members.write();
        for update in updates {
            match members.get_mut(&update.node_id) {
                Some(member) => {
                    // Only apply if incarnation is newer
                    if update.incarnation >= member.incarnation {
                        member.state = update.state;
                        member.incarnation = update.incarnation;
                        member.last_state_change = now_millis();
                    }
                }
                None => {
                    if let Some(addr) = update.address {
                        members.insert(
                            update.node_id.clone(),
                            Member {
                                node_id: update.node_id.clone(),
                                address: addr,
                                state: update.state,
                                incarnation: update.incarnation,
                                last_state_change: now_millis(),
                                metadata: HashMap::new(),
                            },
                        );
                    }
                }
            }
        }
    }

    /// Drain pending updates for piggybacking.
    fn drain_pending_updates(&self) -> Vec<MemberUpdate> {
        let mut pending = self.pending_updates.write();
        std::mem::take(&mut *pending)
    }

    /// Select a random alive member for probing (excluding self).
    pub fn random_target(&self) -> Option<Member> {
        let members = self.members.read();
        let candidates: Vec<_> = members
            .values()
            .filter(|m| {
                m.node_id != self.config.node_id
                    && matches!(m.state, MemberState::Alive | MemberState::Suspected)
            })
            .cloned()
            .collect();

        if candidates.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        candidates.choose(&mut rng).cloned()
    }

    /// Mark a node as suspected (no ACK received).
    pub fn suspect(&self, node_id: &str) {
        let mut members = self.members.write();
        if let Some(member) = members.get_mut(node_id) {
            if member.state == MemberState::Alive {
                member.state = MemberState::Suspected;
                member.last_state_change = now_millis();
                tracing::warn!(node = %node_id, "node suspected");
            }
        }
    }

    /// Mark a node as dead.
    pub fn declare_dead(&self, node_id: &str) {
        let mut members = self.members.write();
        if let Some(member) = members.get_mut(node_id) {
            member.state = MemberState::Dead;
            member.last_state_change = now_millis();
            tracing::error!(node = %node_id, "node declared dead");
        }
    }

    /// Check suspected members and promote to dead if timeout exceeded.
    pub fn check_suspicions(&self) {
        let timeout_ms = self.config.suspicion_timeout.as_millis() as u64;
        let now = now_millis();
        let mut to_declare_dead = Vec::new();

        {
            let members = self.members.read();
            for member in members.values() {
                if member.state == MemberState::Suspected
                    && now - member.last_state_change > timeout_ms
                {
                    to_declare_dead.push(member.node_id.clone());
                }
            }
        }

        for node_id in to_declare_dead {
            self.declare_dead(&node_id);
        }
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(id: &str, port: u16) -> GossipConfig {
        GossipConfig {
            node_id: id.to_string(),
            bind_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            ..GossipConfig::default()
        }
    }

    #[test]
    fn test_join_and_membership() {
        let engine = GossipEngine::new(test_config("node-1", 7001));

        // Self should be alive
        assert_eq!(engine.alive_count(), 1);

        // Another node joins
        engine.handle_message(GossipMessage::Join {
            node_id: "node-2".into(),
            address: "127.0.0.1:7002".parse().unwrap(),
            metadata: HashMap::new(),
        });

        assert_eq!(engine.alive_count(), 2);
    }

    #[test]
    fn test_ping_ack_cycle() {
        let engine = GossipEngine::new(test_config("node-1", 7001));

        engine.add_seed("node-2".into(), "127.0.0.1:7002".parse().unwrap());

        let ping = GossipMessage::Ping {
            sender: "node-2".into(),
            incarnation: 1,
            updates: vec![],
        };

        let response = engine.handle_message(ping);
        assert!(matches!(response, Some(GossipMessage::Ack { .. })));
    }

    #[test]
    fn test_suspicion_and_death() {
        let engine = GossipEngine::new(GossipConfig {
            node_id: "node-1".into(),
            suspicion_timeout: Duration::from_millis(10),
            ..test_config("node-1", 7001)
        });

        engine.add_seed("node-2".into(), "127.0.0.1:7002".parse().unwrap());

        engine.suspect("node-2");
        let members = engine.alive_members();
        assert_eq!(members.len(), 1); // only self is alive

        // Wait for suspicion timeout
        std::thread::sleep(Duration::from_millis(20));
        engine.check_suspicions();

        let all = engine.all_members();
        let node2 = all.iter().find(|m| m.node_id == "node-2").unwrap();
        assert_eq!(node2.state, MemberState::Dead);
    }
}
