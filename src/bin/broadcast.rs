use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    panic,
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
}

struct BroadcastNode {
    node: String,
    id: usize,
    messages: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    neighbours: Vec<String>,
    msg_communicated: HashMap<usize, HashSet<usize>>,
}

impl Node<Payload> for BroadcastNode {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match &input.body.payload {
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
        }
        Ok(())
    }

    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
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
    main_loop::<BroadcastNode, _>()
}
