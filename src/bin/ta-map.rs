use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock, str::FromStr};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn { txn: Vec<Op> },
    TxnOk { txn: Vec<Op> },
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
    map: HashMap<String, isize>,
}

impl Node<Payload> for TAMap {
    fn step(&mut self, event: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("");
        };
        match &input.body.payload {
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
