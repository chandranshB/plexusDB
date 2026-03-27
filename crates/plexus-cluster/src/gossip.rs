//! SWIM-based gossip protocol for cluster membership and failure detection.
//!
//! SWIM (Scalable Weakly-consistent Infection-style Membership) provides:
//! - O(log n) convergence time for membership changes
//! - Bounded false-positive failure detection
//! - Piggybacked state dissemination on ping/ack messages
//!
//! ## Network Transport
//!
//! Messages are serialized with `bincode` and sent over UDP. Each datagram
//! is capped at 1400 bytes (safe MTU). The `run()` method binds a UDP socket
//! and drives the full SWIM protocol loop in a background Tokio task.
//!
//! Protocol:
//! 1. Every `ping_interval`, pick a random member and send PING
//! 2. If no ACK within `ping_timeout` → send PING-REQ to K random members
//! 3. If still no ACK → mark as SUSPECTED
//! 4. After `suspicion_timeout` → mark as DEAD

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

/// Maximum UDP datagram payload (safe below typical MTU).
const MAX_DATAGRAM: usize = 1400;
/// Maximum number of pending membership updates buffered for piggybacking.
/// Older updates are dropped when this limit is reached to bound memory usage.
const MAX_PENDING_UPDATES: usize = 256;

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

                // Broadcast update — cap queue to avoid unbounded growth
                let mut pending = self.pending_updates.write();
                if pending.len() < MAX_PENDING_UPDATES {
                    pending.push(MemberUpdate {
                        node_id,
                        state: MemberState::Alive,
                        incarnation: 0,
                        address: Some(address),
                    });
                }
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

            GossipMessage::PingReq { sender, target } => {
                // Forward a Ping to `target` on behalf of `sender`.
                // We do this in a fire-and-forget Tokio task so the receiver
                // loop is not blocked. The ACK (if any) is relayed back to
                // the original sender.
                let members = self.members.read();
                let target_addr = members.get(&target).map(|m| m.address);
                let sender_addr = members.get(&sender).map(|m| m.address);
                drop(members);

                if let (Some(target_addr), Some(sender_addr)) = (target_addr, sender_addr) {
                    let our_id = self.config.node_id.clone();
                    let incarnation = *self.local_incarnation.read();
                    tokio::spawn(async move {
                        use tokio::net::UdpSocket;
                        let Ok(sock) = UdpSocket::bind("0.0.0.0:0").await else {
                            return;
                        };
                        let ping = GossipMessage::Ping {
                            sender: our_id.clone(),
                            incarnation,
                            updates: vec![],
                        };
                        let Ok(enc) = bincode::serialize(&ping) else {
                            return;
                        };
                        if sock.send_to(&enc, target_addr).await.is_err() {
                            return;
                        }
                        // Wait briefly for ACK from target
                        let mut buf = vec![0u8; MAX_DATAGRAM];
                        let ack = tokio::time::timeout(
                            std::time::Duration::from_millis(500),
                            sock.recv_from(&mut buf),
                        )
                        .await
                        .ok()
                        .and_then(|r| r.ok())
                        .and_then(|(len, _)| {
                            bincode::deserialize::<GossipMessage>(&buf[..len]).ok()
                        })
                        .map(|m| matches!(m, GossipMessage::Ack { .. }))
                        .unwrap_or(false);
                        if ack {
                            // Relay ACK back to original sender
                            let relay_ack = GossipMessage::Ack {
                                sender: our_id,
                                incarnation,
                                updates: vec![],
                            };
                            if let Ok(enc) = bincode::serialize(&relay_ack) {
                                let _ = sock.send_to(&enc, sender_addr).await;
                            }
                        }
                    });
                }
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

    /// Bind a UDP socket and run the full SWIM gossip protocol loop.
    ///
    /// This spawns two Tokio tasks:
    /// - A **receiver** that reads incoming datagrams and dispatches them to
    ///   `handle_message()`, sending any reply back to the sender.
    /// - A **prober** that fires every `ping_interval`, picks a random member,
    ///   sends a PING, waits for an ACK, and marks the member as suspected if
    ///   none arrives within `ping_timeout`.
    ///
    /// Call this once from your async runtime after creating the engine.
    pub async fn run(self: Arc<Self>) -> Result<(), crate::ClusterError> {
        use tokio::net::UdpSocket;

        let socket = UdpSocket::bind(self.config.bind_addr).await.map_err(|e| {
            crate::ClusterError::Network(format!("gossip bind {}: {e}", self.config.bind_addr))
        })?;

        let socket = Arc::new(socket);
        tracing::info!(addr = %self.config.bind_addr, "gossip UDP socket bound");

        // ── Receiver task ────────────────────────────────────────────────────
        let recv_engine = Arc::clone(&self);
        let recv_socket = Arc::clone(&socket);
        tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            loop {
                let (len, src) = match recv_socket.recv_from(&mut buf).await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(error = %e, "gossip recv error");
                        continue;
                    }
                };

                let msg: GossipMessage = match bincode::deserialize(&buf[..len]) {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::debug!(error = %e, src = %src, "gossip: malformed datagram");
                        continue;
                    }
                };

                if let Some(reply) = recv_engine.handle_message(msg) {
                    if let Ok(encoded) = bincode::serialize(&reply) {
                        if encoded.len() <= MAX_DATAGRAM {
                            let _ = recv_socket.send_to(&encoded, src).await;
                        }
                    }
                }
            }
        });

        // ── Prober task ──────────────────────────────────────────────────────
        let probe_engine = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(probe_engine.config.ping_interval);
            loop {
                interval.tick().await;

                // Check suspicion timeouts
                probe_engine.check_suspicions();

                // Pick a random target to probe
                let target = match probe_engine.random_target() {
                    Some(t) => t,
                    None => continue, // single-node cluster
                };

                // Build PING with piggybacked updates
                let updates = probe_engine.drain_pending_updates();
                let incarnation = *probe_engine.local_incarnation.read();
                let ping = GossipMessage::Ping {
                    sender: probe_engine.config.node_id.clone(),
                    incarnation,
                    updates,
                };

                let encoded = match bincode::serialize(&ping) {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if encoded.len() > MAX_DATAGRAM {
                    tracing::warn!("gossip PING too large, skipping");
                    continue;
                }

                // Each probe uses its own ephemeral socket so it doesn't
                // race with the receiver task on the shared listen socket.
                let probe_sock = match tokio::net::UdpSocket::bind("0.0.0.0:0").await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "gossip: failed to bind probe socket");
                        continue;
                    }
                };

                // Send PING
                if probe_sock.send_to(&encoded, target.address).await.is_err() {
                    probe_engine.suspect(&target.node_id);
                    continue;
                }

                // Wait for ACK with timeout
                let timeout = probe_engine.config.ping_timeout;
                let mut ack_buf = vec![0u8; MAX_DATAGRAM];
                let got_ack = tokio::time::timeout(timeout, probe_sock.recv_from(&mut ack_buf))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .and_then(|(len, _src)| {
                        bincode::deserialize::<GossipMessage>(&ack_buf[..len]).ok()
                    })
                    .map(|msg| matches!(msg, GossipMessage::Ack { .. }))
                    .unwrap_or(false);

                if !got_ack {
                    // No direct ACK — try indirect via K random members
                    let indirect_targets: Vec<_> = {
                        let members = probe_engine.members.read();
                        members
                            .values()
                            .filter(|m| {
                                m.node_id != probe_engine.config.node_id
                                    && m.node_id != target.node_id
                                    && m.state == MemberState::Alive
                            })
                            .take(probe_engine.config.indirect_checks)
                            .cloned()
                            .collect()
                    };

                    let mut indirect_ack = false;
                    for relay in &indirect_targets {
                        let req = GossipMessage::PingReq {
                            sender: probe_engine.config.node_id.clone(),
                            target: target.node_id.clone(),
                        };
                        if let Ok(enc) = bincode::serialize(&req) {
                            let _ = probe_sock.send_to(&enc, relay.address).await;
                        }
                    }

                    // Wait briefly for any indirect ACK
                    if !indirect_targets.is_empty() {
                        let indirect_timeout = probe_engine.config.ping_timeout * 2;
                        indirect_ack = tokio::time::timeout(
                            indirect_timeout,
                            probe_sock.recv_from(&mut ack_buf),
                        )
                        .await
                        .ok()
                        .and_then(|r| r.ok())
                        .and_then(|(len, _)| {
                            bincode::deserialize::<GossipMessage>(&ack_buf[..len]).ok()
                        })
                        .map(|msg| matches!(msg, GossipMessage::Ack { .. }))
                        .unwrap_or(false);
                    }

                    if !indirect_ack {
                        probe_engine.suspect(&target.node_id);
                        tracing::warn!(
                            target = %target.node_id,
                            addr = %target.address,
                            "no ACK from target — marking suspected"
                        );
                    }
                }
            }
        });

        Ok(())
    }

    /// Send a Join message to a seed node to bootstrap cluster membership.
    pub async fn join_via_udp(
        self: &Arc<Self>,
        seed_addr: SocketAddr,
    ) -> Result<(), crate::ClusterError> {
        use tokio::net::UdpSocket;

        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| crate::ClusterError::Network(e.to_string()))?;

        let join_msg = GossipMessage::Join {
            node_id: self.config.node_id.clone(),
            address: self.config.bind_addr,
            metadata: HashMap::new(),
        };

        let encoded = bincode::serialize(&join_msg)
            .map_err(|e| crate::ClusterError::Network(e.to_string()))?;

        socket
            .send_to(&encoded, seed_addr)
            .await
            .map_err(|e| crate::ClusterError::Network(e.to_string()))?;

        tracing::info!(seed = %seed_addr, node = %self.config.node_id, "sent Join to seed node");
        Ok(())
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
