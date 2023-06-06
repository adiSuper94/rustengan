use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    panic,
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Read,
    Broadcast {
        message: usize,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    ReadOk {
        messages: Vec<usize>,
    },
    BroadcastOk,
    TopologyOk,
    Gossip {
        new_messages: Vec<usize>,
    },
    GossipOk,
}

enum InjectedPayload {
    Gossip,
}

struct BroadcastNode {
    node: String,
    id: usize,
    messages: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    neighbours: Vec<String>,
    msg_communicated: HashMap<usize, HashSet<usize>>,
}

impl Node<Payload, InjectedPayload> for BroadcastNode {
    fn step(
        &mut self,
        event: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match &event {
            Event::Message(input) => match &input.body.payload {
                Payload::Read => {
                    let reply = input.construct_reply(
                        Payload::ReadOk {
                            messages: self.messages.clone().into_iter().collect(),
                        },
                        Some(&mut self.id),
                    );
                    self.send(&reply, output)?;
                }
                Payload::Broadcast { message } => {
                    self.messages.insert(Clone::clone(message));
                    let reply = input.construct_reply(Payload::BroadcastOk, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::Topology { topology } => {
                    self.neighbours = topology
                        .clone()
                        .remove(&self.node)
                        .unwrap_or_else(|| panic!("no topology give for node {}", &self.node));
                    let reply = input.construct_reply(Payload::TopologyOk, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::ReadOk { .. } | Payload::BroadcastOk | Payload::TopologyOk => {}
                Payload::Gossip { new_messages } => {
                    self.messages.extend(new_messages);
                    let reply = input.construct_reply(Payload::GossipOk, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::GossipOk => {
                    if let Some(in_reply_to) = &input.body.in_reply_to {
                        if self.msg_communicated.contains_key(in_reply_to) {
                            let communicated_messages =
                                self.msg_communicated.remove(in_reply_to).unwrap();

                            self.known
                                .entry(input.src.clone())
                                .or_default()
                                .extend(communicated_messages);
                        }
                    }
                }
            },
            Event::InjectedPayload(payload) => match &payload {
                InjectedPayload::Gossip => {
                    for neighbour in &self.neighbours {
                        let neighbour_known = self.known.entry(neighbour.clone()).or_default();
                        let new_messages: Vec<usize> =
                            self.messages.difference(neighbour_known).cloned().collect();
                        let message = Message {
                            dest: neighbour.clone(),
                            src: self.node.clone(),
                            body: Body {
                                payload: Payload::Gossip {
                                    new_messages: new_messages.clone(),
                                },
                                id: Some(self.id),
                                in_reply_to: None,
                            },
                        };
                        self.msg_communicated
                            .entry(self.id)
                            .or_default()
                            .extend(new_messages);
                        self.id += 1;
                        self.send(&message, output)?;
                    }
                }
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
                std::thread::sleep(Duration::from_millis(500));
                if tx
                    .send(Event::InjectedPayload(InjectedPayload::Gossip))
                    .is_err()
                {
                    break;
                }
            }
        });
        let node = BroadcastNode {
            node: init.node_id,
            id: 1,
            messages: HashSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
            msg_communicated: HashMap::new(),
            neighbours: Vec::new(),
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<BroadcastNode, _, _>()
}
