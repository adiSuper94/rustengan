use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock, str::FromStr, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn { txn: Vec<Op> },
    TxnOk { txn: Vec<Op> },
    Gossip { txn: Txn },
    GossipOk,
}

enum InjectedPayload {
    Gossip,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum OpType {
    Read,
    Write,
}

#[derive(Debug, Clone)]
struct Op {
    op_type: OpType,
    key: isize,
    val: Option<isize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Txn {
    ops: Vec<Op>,
}

impl FromStr for OpType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "r" => Ok(OpType::Read),
            "w" => Ok(OpType::Write),
            _ => Err(anyhow::anyhow!("Invalid op type")),
        }
    }
}
impl Serialize for Op {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let op = match &self.op_type {
            OpType::Read => "r".to_string(),
            OpType::Write => "w".to_string(),
        };
        (op, &self.key, &self.val).serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Op {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(
            |(ot, key, val): (String, isize, Option<isize>)| {
                let op_type = OpType::from_str(ot.as_ref()).unwrap();
                Op { op_type, key, val }
            },
        )
    }
}
struct TAMap {
    id: usize,
    node: String,
    nodes: Vec<String>,
    map: HashMap<String, isize>,
    txns_to_gossip: HashMap<String, Vec<Txn>>,
    gossiped_txn: HashMap<usize, (String, Vec<Txn>)>,
}

impl Node<Payload, InjectedPayload> for TAMap {
    fn step(
        &mut self,
        event: rustengan::Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match &event {
            Event::Message(input) => match &input.body.payload {
                Payload::Txn { txn } => {
                    let mut response_txn: Vec<Op> = Vec::new();
                    for op in txn {
                        match op.op_type {
                            OpType::Read => {
                                let val = self.map.get(&op.key.to_string()).cloned();
                                response_txn.push(Op {
                                    op_type: op.op_type.clone(),
                                    key: op.key.clone(),
                                    val,
                                });
                            }
                            OpType::Write => {
                                if let Some(val) = op.val {
                                    self.map.insert(op.key.to_string(), val);
                                    response_txn.push(op.clone());
                                }
                            }
                        }
                    }
                    for node in &self.nodes {
                        if &self.node != node {
                            self.txns_to_gossip
                                .entry(node.clone())
                                .or_insert_with(Vec::new)
                                .push(Txn { ops: txn.clone() });
                        }
                    }

                    let reply = input
                        .construct_reply(Payload::TxnOk { txn: response_txn }, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::TxnOk { txn: _txn } => {}
                Payload::Gossip { txn } => {
                    for op in &txn.ops {
                        match op.op_type {
                            OpType::Read => {}
                            OpType::Write => {
                                if let Some(val) = op.val {
                                    self.map.insert(op.key.to_string(), val);
                                }
                            }
                        }
                    }
                    let reply = input.construct_reply(Payload::GossipOk, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::GossipOk => {
                    let ref_msg_id = input.body.in_reply_to.unwrap();
                    self.gossiped_txn.remove(&ref_msg_id);
                }
            },
            Event::InjectedPayload(injected_input) => match injected_input {
                InjectedPayload::Gossip => {
                    if let Some(node) = self.txns_to_gossip.keys().next().cloned() {
                        let txns = self.txns_to_gossip.get(&node).unwrap().clone();
                        for t in &txns {
                            let payload = Payload::Gossip { txn: t.clone() };
                            let msg = Message {
                                src: self.node.clone(),
                                dest: node.clone(),
                                body: Body {
                                    payload,
                                    id: Some(self.id),
                                    in_reply_to: None,
                                },
                            };
                            self.send(&msg, output)?;
                            self.gossiped_txn
                                .insert(self.id, (node.clone(), txns.clone()));
                            self.id += 1;
                        }
                        self.txns_to_gossip.remove(&node);
                    }
                }
            },
            Event::EOF => todo!(),
        }

        Ok(())
    }

    fn from_init(
        init: Init,
        tx: std::sync::mpsc::Sender<rustengan::Event<Payload, InjectedPayload>>,
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
        let node = TAMap {
            id: 1,
            nodes: init
                .node_ids
                .into_iter()
                .filter(|n| n != &init.node_id)
                .collect(),
            node: init.node_id,
            map: HashMap::new(),
            txns_to_gossip: HashMap::new(),
            gossiped_txn: HashMap::new(),
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<TAMap, _, _>()
}
