use anyhow::{bail, Context};
use rustengan::*;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {},
}

struct UniqNode {
    id: usize,
}

impl Node<Payload> for UniqNode {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Generate {} => {
                let guid = ulid::Ulid::new().to_string();
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
            Payload::Init { .. } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk {},
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("Serialize response to Echo")?;
                output.write_all(b"\n").context("write tailing new line")?;
                self.id += 1;
            }
            Payload::InitOk {} => bail!("received init_ok"),
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let state = UniqNode { id: 0 };
    main_loop(state)
}
