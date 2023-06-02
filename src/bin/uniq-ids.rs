use anyhow::Context;
use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{
    format,
    io::{StdoutLock, Write},
};

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
        match input.body.payload {
            Payload::Generate {} => {
                let guid = format!("{}-{}", self.node, self.id);
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk { guid },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("Serialize response to Generate")?;
                output.write_all(b"\n").context("write tailing new line")?;
                self.id += 1;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }

    fn from_init(_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let state = UniqNode {
            id: 1,
            node: init.node_id,
        };
        Ok(state)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqNode, _>(())
}
