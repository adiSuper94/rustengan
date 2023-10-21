use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock, str::FromStr};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn {
        txn: Vec<(String, isize, Option<isize>)>,
    },
    TxnOk {
        txn: Vec<(String, isize, Option<isize>)>,
    },
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
// enum OpType {
//     Read("r"),
//     Write("w"),
// }

// impl FromStr for OpType {
//     type Err = anyhow::Error;
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s {
//             "r" => Ok(OpType::Read),
//             "w" => Ok(OpType::Write),
//             _ => Err(anyhow::anyhow!("Invalid op type")),
//         }
//     }
// }
// #[derive(Debug, Clone, Serialize, Deserialize)]
// struct Op {
//     op_type: OpType,
//     key: String,
//     val: Option<isize>,
// }

struct TAMap {
    id: usize,
    node: String,
    map: HashMap<String, isize>,
}

impl Node<Payload> for TAMap {
    fn step(&mut self, event: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("");
        };
        match &input.body.payload {
            Payload::Txn { txn } => {
                let mut response_txn: Vec<(String, isize, Option<isize>)> = Vec::new();
                for op in txn {
                    if op.0 == "r" {
                        let val = self.map.get(&op.1.to_string()).cloned();
                        response_txn.push((op.0.to_string(), op.1.clone(), val));
                    } else if op.0 == "w" {
                        if let Some(val) = op.2 {
                            self.map.insert(op.1.to_string(), val);
                            response_txn.push((op.0.to_string(), op.1.clone(), Some(val)));
                        }
                    }
                }

                let reply =
                    input.construct_reply(Payload::TxnOk { txn: response_txn }, Some(&mut self.id));
                self.send(&reply, output)?;
            }
            Payload::TxnOk { txn: _txn } => {}
        }
        Ok(())
    }

    fn from_init(init: Init, _tx: std::sync::mpsc::Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node = TAMap {
            id: 1,
            node: init.node_id,
            map: HashMap::new(),
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<TAMap, _, _>()
}
