use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

enum InjectedPayload {
    Gossip,
}

struct KLogNode {
    node: String,
    nodes: Vec<String>,
    id: usize,
    logs: HashMap<String, HashMap<usize, usize>>,
    next_offset: usize,
    processed_till: HashMap<String, usize>,
}

impl KLogNode {
    pub fn append_log(&mut self, key: String, msg: usize) -> usize {
        let log = self.logs.entry(key).or_insert(HashMap::new());
        log.insert(self.next_offset, msg);
        self.next_offset += 1;
        self.next_offset - 1
    }
}

impl Node<Payload, InjectedPayload> for KLogNode {
    fn step(
        &mut self,
        event: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match &event {
            Event::Message(input) => match &input.body.payload {
                Payload::Send { key, msg } => {
                    let offset = self.append_log(key.to_string(), msg.clone());
                    let reply =
                        input.construct_reply(Payload::SendOk { offset }, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::Poll { offsets } => {
                    let mut msgs: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
                    for (key, offset) in offsets.iter() {
                        let mut msgs_after_offset: Vec<(usize, usize)> = Vec::new();
                        if let Some(log) = self.logs.get(key) {
                            for (log_offset, msg) in log.iter() {
                                if log_offset >= offset {
                                    msgs_after_offset.push((log_offset.clone(), msg.clone()));
                                }
                            }
                            msgs.insert(key.to_string(), msgs_after_offset);
                        }
                    }
                    let reply = input.construct_reply(Payload::PollOk { msgs }, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::CommitOffsets { offsets } => {
                    for (key, offset) in offsets.iter() {
                        if let Some(_log) = self.logs.get(key) {
                            self.processed_till.insert(key.to_string(), offset.clone());
                        }
                    }
                    let reply = input.construct_reply(Payload::CommitOffsetsOk, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::ListCommittedOffsets { keys } => {
                    let mut commited_offsets: HashMap<String, usize> = HashMap::new();
                    for key in keys.iter() {
                        if let Some(commited_offset) = self.processed_till.get(key) {
                            commited_offsets.insert(key.to_string(), commited_offset.clone());
                        }
                    }
                    let reply = input.construct_reply(
                        Payload::ListCommittedOffsetsOk {
                            offsets: commited_offsets,
                        },
                        Some(&mut self.id),
                    );
                    self.send(&reply, output)?;
                }
                Payload::SendOk { .. }
                | Payload::PollOk { .. }
                | Payload::CommitOffsetsOk
                | Payload::ListCommittedOffsetsOk { .. } => {}
            },
            Event::InjectedPayload(injected_payload) => match &injected_payload {
                InjectedPayload::Gossip => {}
            },
            Event::EOF => {}
        }
        Ok(())
    }

    fn from_init(
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        std::thread::spawn(move || {
            // TODO: Handle EOF
            loop {
                std::thread::sleep(Duration::from_millis(200));
                if tx
                    .send(Event::InjectedPayload(InjectedPayload::Gossip))
                    .is_err()
                {
                    break;
                }
            }
        });
        let node = KLogNode {
            id: 1,
            logs: HashMap::new(),
            next_offset: 0,
            processed_till: HashMap::new(),
            nodes: init
                .node_ids
                .into_iter()
                .filter(|n| n != &init.node_id)
                .collect(),
            node: init.node_id,
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<KLogNode, _, _>()
}
