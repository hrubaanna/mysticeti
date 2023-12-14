// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;
use std::{collections::HashMap, env, sync::Arc, time::Duration};

use futures::future::join_all;
use itertools::Itertools;
use rand::{seq::SliceRandom, thread_rng};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::commit_observer::CommitObserver;
use crate::committee::Committee;
use crate::config::SynchronizerParameters;
use crate::{
    block_handler::BlockHandler,
    metered_channel,
    metrics::Metrics,
    net_sync::{self, NetworkSyncerInner},
    network::NetworkMessage,
    runtime::{sleep, Handle, JoinHandle},
    types::{AuthorityIndex, BlockReference, RoundNumber},
};

pub struct BlockDisseminator<H: BlockHandler, C: CommitObserver + 'static> {
    /// The sender to the network.
    sender: metered_channel::Sender<NetworkMessage>,
    /// The inner state of the network syncer.
    inner: Arc<NetworkSyncerInner<H, C>>,
    /// The handle of the task disseminating our own blocks.
    own_blocks: Option<JoinHandle<Option<()>>>,
    /// The handles of tasks disseminating other nodes' blocks.
    other_blocks: Vec<JoinHandle<Option<()>>>,
    /// The parameters of the synchronizer.
    parameters: SynchronizerParameters,
    /// Metrics.
    metrics: Arc<Metrics>,
    /// The peer id
    _peer: AuthorityIndex,
    _committee: Arc<Committee>,
}

impl<H, C> BlockDisseminator<H, C>
where
    H: BlockHandler + 'static,
    C: CommitObserver + 'static,
{
    pub fn new(
        _peer: AuthorityIndex,
        _committee: Arc<Committee>,
        sender: metered_channel::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<H, C>>,
        metrics: Arc<Metrics>,
        parameters: SynchronizerParameters,
    ) -> Self {
        Self {
            sender,
            inner,
            own_blocks: None,
            other_blocks: Vec::new(),
            parameters,
            metrics,
            _peer,
            _committee,
        }
    }

    pub async fn shutdown(mut self) {
        let mut waiters = Vec::with_capacity(1 + self.other_blocks.len());
        if let Some(handle) = self.own_blocks.take() {
            handle.abort();
            waiters.push(handle);
        }
        for handle in self.other_blocks {
            handle.abort();
            waiters.push(handle);
        }
        join_all(waiters).await;
    }

    pub async fn send_blocks(
        &mut self,
        peer: AuthorityIndex,
        peer_addr: SocketAddr,
        references: Vec<BlockReference>,
    ) -> Option<()> {
        let mut missing = Vec::new();
        const CHUNK_SIZE: usize = 10;
        let mut to_send = vec![];
        for reference in references {
            let stored_block = self.inner.block_store.get_block(reference);
            let found = stored_block.is_some();

            match stored_block {
                Some(block) => to_send.push(block),
                None => missing.push(reference),
            }

            if to_send.len() >= CHUNK_SIZE {
                self.send(
                    peer_addr,
                    NetworkMessage::MissingInclusionsResponse(std::mem::take(&mut to_send)),
                )?;
            }

            self.metrics
                .block_sync_requests_received
                .with_label_values(&[&peer.to_string(), &found.to_string()])
                .inc();
        }

        // send any leftovers
        if !to_send.is_empty() {
            self.send(
                peer_addr,
                NetworkMessage::MissingInclusionsResponse(std::mem::take(&mut to_send)),
            )?;
        }

        self.send(peer_addr, NetworkMessage::BlockNotFound(missing))
    }

    fn send(&self, peer: SocketAddr, message: NetworkMessage) -> Option<()> {
        match self.sender.try_reserve() {
            Err(TrySendError::Full(_)) => {
                tracing::error!("Channel full to {}, dropping message", peer);
            }
            Err(TrySendError::Closed(_)) => {
                return None;
            }
            Ok(permit) => {
                permit.send(message);
            }
        }
        Some(())
    }

    pub async fn disseminate_own_blocks(&mut self, round: RoundNumber) {
        if let Some(existing) = self.own_blocks.take() {
            existing.abort();
            existing.await.ok();
        }

        let handle = Handle::current().spawn(Self::stream_own_blocks(
            self.sender.clone(),
            self.inner.clone(),
            round,
            self.parameters.batch_size,
        ));
        self.own_blocks = Some(handle);
    }

    async fn stream_own_blocks(
        to: metered_channel::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<H, C>>,
        _round: RoundNumber,
        batch_size: usize,
    ) -> Option<()> {
        // continue not from where the node left off - let that to the synchronizer to fetch the missing blocks - but keep sending
        // from the latest block only.
        let mut round = inner
            .block_store
            .last_own_block_ref()
            .map(|block| block.round)
            .unwrap_or(0)
            .saturating_sub(1);
        loop {
            let notified = inner.notify.notified();
            let blocks = inner.block_store.get_own_blocks(round, batch_size);
            if !blocks.is_empty() {
                round = blocks.last().unwrap().round();
                to.send(NetworkMessage::Blocks(blocks)).await.ok()?;
            }
            notified.await
        }
    }

    // TODO:
    // * There should be a new protocol message that indicate when we should stop this task.
    // * Decide when to subscribe to a stream versus requesting specific blocks by ids.
    #[allow(dead_code)]
    pub fn disseminate_others_blocks(&mut self, round: RoundNumber, author: AuthorityIndex) {
        if self.other_blocks.len() >= self.parameters.maximum_helpers_per_authority {
            return;
        }

        let handle = Handle::current().spawn(Self::stream_others_blocks(
            self.sender.clone(),
            self.inner.clone(),
            round,
            author,
            self.parameters.batch_size,
            self.parameters.stream_interval,
        ));
        self.other_blocks.push(handle);
    }

    async fn stream_others_blocks(
        to: metered_channel::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<H, C>>,
        mut round: RoundNumber,
        author: AuthorityIndex,
        batch_size: usize,
        stream_interval: Duration,
    ) -> Option<()> {
        loop {
            let blocks = inner
                .block_store
                .get_others_blocks(round, author, batch_size);
            if !blocks.is_empty() {
                round = blocks.last().unwrap().round();
                to.send(NetworkMessage::Blocks(blocks)).await.ok()?;
            }
            sleep(stream_interval).await;
        }
    }
}

enum BlockFetcherMessage {
    RegisterAuthority(
        AuthorityIndex,
        metered_channel::Sender<NetworkMessage>,
        tokio::sync::watch::Receiver<Duration>,
    ),
    RemoveAuthority(AuthorityIndex),
}

pub struct BlockFetcher {
    sender: mpsc::Sender<BlockFetcherMessage>,
    handle: JoinHandle<Option<()>>,
}

impl BlockFetcher {
    pub fn start<B, C>(
        id: AuthorityIndex,
        inner: Arc<NetworkSyncerInner<B, C>>,
        metrics: Arc<Metrics>,
    ) -> Self
    where
        B: BlockHandler + 'static,
        C: CommitObserver + 'static,
    {
        let (sender, receiver) = mpsc::channel(100);
        let worker = BlockFetcherWorker::new(id, inner, receiver, metrics);
        let handle = Handle::current().spawn(worker.run());
        Self { sender, handle }
    }

    pub async fn register_authority(
        &self,
        authority: AuthorityIndex,
        sender: metered_channel::Sender<NetworkMessage>,
        latency_receiver: tokio::sync::watch::Receiver<Duration>,
    ) {
        self.sender
            .send(BlockFetcherMessage::RegisterAuthority(
                authority,
                sender,
                latency_receiver,
            ))
            .await
            .ok();
    }

    pub async fn remove_authority(&self, authority: AuthorityIndex) {
        self.sender
            .send(BlockFetcherMessage::RemoveAuthority(authority))
            .await
            .ok();
    }

    pub async fn shutdown(self) {
        self.handle.abort();
        self.handle.await.ok();
    }
}

struct BlockFetcherWorker<B: BlockHandler, C: CommitObserver + 'static> {
    id: AuthorityIndex,
    inner: Arc<NetworkSyncerInner<B, C>>,
    receiver: mpsc::Receiver<BlockFetcherMessage>,
    senders: HashMap<
        AuthorityIndex,
        (
            metered_channel::Sender<NetworkMessage>,
            tokio::sync::watch::Receiver<Duration>,
        ),
    >,
    parameters: SynchronizerParameters,
    metrics: Arc<Metrics>,
    enable: bool,
}

impl<B, C> BlockFetcherWorker<B, C>
where
    B: BlockHandler + 'static,
    C: CommitObserver + 'static,
{
    pub fn new(
        id: AuthorityIndex,
        inner: Arc<NetworkSyncerInner<B, C>>,
        receiver: mpsc::Receiver<BlockFetcherMessage>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let enable = env::var("USE_SYNCER").is_ok();
        Self {
            id,
            inner,
            receiver,
            senders: Default::default(),
            parameters: Default::default(),
            metrics,
            enable,
        }
    }

    async fn run(mut self) -> Option<()> {
        loop {
            tokio::select! {
                _ = sleep(self.parameters.sample_precision) => self.sync_strategy().await,
                message = self.receiver.recv() => {
                    match message {
                        Some(BlockFetcherMessage::RegisterAuthority(authority, sender, latency_receiver)) => {
                            self.senders.insert(authority, (sender, latency_receiver));
                        },
                        Some(BlockFetcherMessage::RemoveAuthority(authority)) => {
                            self.senders.remove(&authority);
                        },
                        None => return None,
                    }
                }
            }
        }
    }

    /// A simple and naive strategy that requests missing blocks from random peers.
    async fn sync_strategy(&self) {
        if self.enable {
            return;
        }
        let mut to_request: Vec<BlockReference> = Vec::new();
        let missing_blocks = self.inner.syncer.get_missing_blocks().await;
        for (authority, missing) in missing_blocks.into_iter().enumerate() {
            self.metrics
                .missing_blocks
                .with_label_values(&[&authority.to_string()])
                .inc_by(missing.len() as u64);

            // TODO: If we are missing many blocks from the same authority
            // (`missing.len() > self.parameters.new_stream_threshold`), it is likely that
            // we have a network partition. We should try to find an other peer from which
            // to (temporarily) sync the blocks from that authority.

            to_request.extend(missing.into_iter().collect::<Vec<_>>());
        }

        // just sort them by ascending order to help facilitate the processing once responses arrive
        to_request = to_request
            .into_iter()
            .sorted_by(|b1, b2| Ord::cmp(&b1.round, &b2.round))
            .collect::<Vec<_>>();

        for chunks in to_request.chunks(net_sync::MAXIMUM_BLOCK_REQUEST) {
            let Some((peer, permit)) = self.sample_peer(&[self.id]) else {
                break;
            };
            let message = NetworkMessage::RequestBlocks(chunks.to_vec());
            permit.send(message);

            self.metrics
                .block_sync_requests_sent
                .with_label_values(&[&peer.to_string()])
                .inc();
        }
    }

    fn sample_peer(
        &self,
        except: &[AuthorityIndex],
    ) -> Option<(AuthorityIndex, metered_channel::Permit<NetworkMessage>)> {
        static MILIS_IN_MINUTE: u128 = Duration::from_secs(60).as_millis();
        let senders = self
            .senders
            .iter()
            .filter(|&(index, _)| !except.contains(index))
            .map(|(index, (sender, latency_receiver))| {
                (
                    index,
                    sender,
                    MILIS_IN_MINUTE.saturating_sub(latency_receiver.borrow().as_millis()) as f64,
                )
            })
            .collect::<Vec<_>>();

        static NUMBER_OF_PEERS: usize = 6;
        let senders = senders
            .choose_multiple_weighted(&mut thread_rng(), NUMBER_OF_PEERS, |item| item.2)
            .expect("Weighted choice error: latency values incorrect!")
            .collect::<Vec<_>>();

        for (peer, sender, _latency) in senders {
            if let Ok(permit) = sender.try_reserve() {
                return Some((**peer, permit));
            }
        }
        None
    }
}
