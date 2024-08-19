// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashSet, HashMap};
use std::fmt;

use crate::block_store::{self, BlockStore};
use crate::core::CommitMessage;
use crate::validator::Validator;
use crate::{
    data::Data,
    types::{BlockReference, StatementBlock},
};

use tokio::sync::mpsc;
use std::sync::{Arc, Mutex};

/// The output of consensus is an ordered list of [`CommittedSubDag`]. The application can arbitrarily
/// sort the blocks within each sub-dag (but using a deterministic algorithm).
pub struct CommittedSubDag {
    /// A reference to the anchor of the sub-dag
    pub anchor: BlockReference,
    /// All the committed blocks that are part of this sub-dag
    pub blocks: Vec<Data<StatementBlock>>,
}

impl CommittedSubDag {
    /// Create new (empty) sub-dag.
    pub fn new(anchor: BlockReference, blocks: Vec<Data<StatementBlock>>) -> Self {
        Self { anchor, blocks }
    }

    /// Sort the blocks of the sub-dag by round number. Any deterministic algorithm works.
    pub fn sort(&mut self) {
        self.blocks.sort_by_key(|x| x.round());
    }
}

/// Expand a committed sequence of leader into a sequence of sub-dags.
#[derive(Default)]
pub struct Linearizer {
    /// Keep track of all committed blocks to avoid committing the same block twice.
    pub committed: HashSet<BlockReference>,
    commit_messages: Mutex<HashMap<u64, HashMap<ValidatorId, Data<StatementBlock>>>>,
    num_validators: usize,
}

impl Linearizer {
    pub fn new(num_validators: usize) -> Self {
        Self {
            committed: HashSet::new(),
            commit_messages: Mutex::new(HashMap::new()),
            num_validators,
        }
    }

    /// Collect the sub-dag from a specific anchor excluding any duplicates or blocks that
    /// have already been committed (within previous sub-dags).
    fn collect_sub_dag(
        &mut self,
        block_store: &BlockStore,
        leader_block: Data<StatementBlock>,
    ) -> CommittedSubDag {
        let mut to_commit = Vec::new();

        let leader_block_ref = *leader_block.reference();
        let mut buffer = vec![leader_block];
        assert!(self.committed.insert(leader_block_ref));
        while let Some(x) = buffer.pop() {
            to_commit.push(x.clone());
            for reference in x.includes() {
                // The block manager may have cleaned up blocks passed the latest committed rounds.
                let block = block_store
                    .get_block(*reference)
                    .expect("We should have the whole sub-dag by now");

                // Skip the block if we already committed it (either as part of this sub-dag or
                // a previous one).
                if self.committed.insert(*reference) {
                    buffer.push(block);
                }
            }
        }
        CommittedSubDag::new(leader_block_ref, to_commit)
    }

    // Handle commit messages by grouping them by round number and sorting them once all validators have sent their messages for the round.
    pub fn handle_commit(
        &mut self,
        block_store: &BlockStore,
        round: u64,
        validator_id: ValidatorId,
        block: Data<StatementBlock>,
    ) -> Vec<CommittedSubDag> {
        let mut commit_messages = self.commit_messages.lock().unwrap();

        // Group commit messages by round number
        let round_messages = commit_messages.entry(round);
        round_messages.insert(validator_id, block);

        // Check if we have received commit messages from all validators for this round
        if round_messages.len() == self.num_validators {
            // Collect and sort the commit messages for this round
            let mut committed_leaders: Vec<Data<StatementBlock>> = round_messages.values().cloned().collect();
            committed_leaders.sort_by_key(|x| x.round()); // Sort the blocks by round number

            // Process each sorted commit message
            for leader_block in committed_leaders {
                // Collect the sub-dag generated using each of these leaders as anchor.
                let mut sub_dag = self.collect_sub_dag(block_store, leader_block);

                // Sort the sub-dag using a deterministic algorithm.
                sub_dag.sort();
                committed.push(sub_dag);            
        }

        // Remove the processed round from the commit messages
        commit_messages.remove(&round);

        }

        committed
    }
}

pub struct LinearizerTask {
    linearizer: Linearizer,
    receiver: mpsc::Receiver<BlockReference>,
}

impl LinearizerTask {
    pub fn new(receiver: mpsc::Receiver<BlockReference>, linearizer: Linearizer) -> Self {
        Self { receiver, linearizer }
    }

    pub async fn run(mut self) {
        while let Some(block_reference) = self.receiver.recv().await {
            // Process the block reference
            // Assuming each validator manages its own BlockStore
            if let Some(block) = BlockStore::get_block(block_reference) {
                let (validator_id, round) = block_reference.author_round();

                // Handle the commit
                self.linearizer.handle_commit(&self.block_store, round, validator_id, block);
            }
        }
    }
}

impl fmt::Debug for CommittedSubDag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.anchor)?;
        for block in &self.blocks {
            write!(f, "{}, ", block.reference())?;
        }
        write!(f, ")")
    }
}
