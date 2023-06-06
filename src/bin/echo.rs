use rustengan::*;
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize,
}

impl Node<Payload> for EchoNode {
    fn step(&mut self, event: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = event else{panic!("");};
        match &input.body.payload {
            Payload::Echo { echo } => {
                let reply = input
                    .construct_reply(Payload::EchoOk { echo: echo.clone() }, Some(&mut self.id));
                self.send(&reply, output)?;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }

    fn from_init(_init: Init, _tx: std::sync::mpsc::Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(EchoNode { id: 1 })
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<EchoNode, _, _>()
}
