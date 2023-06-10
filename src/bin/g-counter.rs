use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    Gossip { values: HashMap<String, usize> },
    GossipOk { value: usize },
}

enum InjectedPayload {
    Gossip,
}

struct GCounterNode {
    node: String,
    nodes: Vec<String>,
    id: usize,
    values: HashMap<String, usize>,
    ack: HashMap<String, usize>,
}

impl Node<Payload, InjectedPayload> for GCounterNode {
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
                            value: self.values.values().sum(),
                        },
                        Some(&mut self.id),
                    );
                    self.send(&reply, output)?;
                }
                Payload::Add { delta } => {
                    let value = self.values.entry(self.node.clone()).or_default();
                    *value += delta;
                    let reply = input.construct_reply(Payload::AddOk, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::Gossip { values } => {
                    for k in &self.nodes {
                        if let Some(v) = values.get(k) {
                            if v > self.values.get(k).unwrap_or(&0) {
                                self.values.insert(k.to_string(), *v);
                            }
                        }
                        let reply = input.construct_reply(
                            Payload::GossipOk {
                                value: Clone::clone(self.values.get(&input.src).unwrap_or(&0)),
                            },
                            Some(&mut self.id),
                        );
                        self.send(&reply, output)?;
                    }
                }
                Payload::AddOk | Payload::ReadOk { .. } => {}
                Payload::GossipOk { value } => {
                    self.ack.insert(input.src.clone(), *value);
                }
            },
            Event::InjectedPayload(payload) => match payload {
                InjectedPayload::Gossip => {
                    for n in &self.nodes {
                        if self.ack.get(n).unwrap_or(&0)
                            != self.values.get(&self.node).unwrap_or(&0)
                        {
                            let message = Message {
                                src: self.node.clone(),
                                dest: n.to_string(),
                                body: Body {
                                    payload: Payload::Gossip {
                                        values: self.values.clone(),
                                    },
                                    id: Some(self.id),
                                    in_reply_to: None,
                                },
                            };
                            self.send(&message, output)?;
                            self.id += 1;
                        }
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
                std::thread::sleep(Duration::from_millis(200));
                if tx
                    .send(Event::InjectedPayload(InjectedPayload::Gossip))
                    .is_err()
                {
                    break;
                }
            }
        });
        let node = GCounterNode {
            id: 1,
            values: HashMap::new(),
            nodes: init
                .node_ids
                .into_iter()
                .filter(|n| n != &init.node_id)
                .collect(),
            node: init.node_id,
            ack: HashMap::new(),
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<GCounterNode, _, _>()
}
