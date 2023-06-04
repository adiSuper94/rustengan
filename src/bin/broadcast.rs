use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock};

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
    id: usize,
    messages: Vec<usize>,
}

impl Node<(), Payload> for BroadcastNode {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match &input.body.payload {
            Payload::Read => {
                input.reply(
                    Payload::ReadOk {
                        messages: self.messages.clone(),
                    },
                    Some(&mut self.id),
                    output,
                )?;
            }
            Payload::Broadcast { message } => {
                self.messages.push(message.clone());
                input.reply(Payload::BroadcastOk, Some(&mut self.id), output)?;
            }
            Payload::Topology { topology: _ } => {
                input.reply(Payload::TopologyOk, Some(&mut self.id), output)?;
            }
            Payload::ReadOk { .. } | Payload::BroadcastOk | Payload::TopologyOk => {}
        }
        Ok(())
    }

    fn from_init(_state: (), _init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node = BroadcastNode {
            id: 1,
            messages: Vec::new(),
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
