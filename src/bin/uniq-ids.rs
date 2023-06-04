use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{format, io::StdoutLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqNode {
    id: usize,
    node: String,
}

impl Node<(), Payload> for UniqNode {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match &input.body.payload {
            Payload::Generate {} => {
                let guid = format!("{}-{}", self.node, self.id);
                input.reply(Payload::GenerateOk { guid }, Some(&mut self.id), output)?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }

    fn from_init(_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node = UniqNode {
            id: 1,
            node: init.node_id,
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqNode, _>(())
}
