use std::io::{StdoutLock, Write};

use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    InjectedPayload(InjectedPayload),
    EOF,
}

impl<Payload> Message<Payload>
where
    Payload: Serialize,
{
    pub fn construct_reply(&self, payload: Payload, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body: Body {
                id: match id {
                    Some(val) => {
                        let old = *val;
                        *val += 1;
                        Some(old)
                    }
                    None => None,
                },
                in_reply_to: self.body.id,
                payload,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Node<P, IP = ()> {
    fn from_init(init: Init, tx: std::sync::mpsc::Sender<Event<P, IP>>) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn step(&mut self, input: Event<P, IP>, output: &mut StdoutLock) -> anyhow::Result<()>;

    fn send(&self, message: &Message<P>, output: &mut StdoutLock) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        serde_json::to_writer(&mut *output, message).context("Serialize response")?;
        output.write_all(b"\n").context("write tailing new line")?;
        Ok(())
    }
}

pub fn main_loop<N, P, IP>() -> anyhow::Result<()>
where
    N: Node<P, IP>,
    P: DeserializeOwned + Send + 'static,
    IP: Send + 'static,
{
    // WTF is DeserializedOwned??
    let mut stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();
    let init_msg = serde_json::Deserializer::from_reader(&mut stdin)
        .into_iter::<Message<InitPayload>>()
        .next()
        .expect("no init message received")
        .context("could not serialize init message")?;
    let InitPayload::Init(init) = init_msg.body.payload else {
    panic!("First message was not Init");
    };
    let reply = Message {
        src: init_msg.dest,
        dest: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };
    serde_json::to_writer(&mut stdout, &reply).context("serialize response to init")?;
    stdout.write_all(b"\n").context("write trailing newline")?;
    let (tx, rx) = std::sync::mpsc::channel::<Event<P, IP>>();
    let mut node: N = Node::from_init(init, tx.clone()).context("node initialization failed")?;
    drop(stdin);
    let th = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<P>>();
        for input in inputs {
            let input = input.context("could not deser input from STDIN")?;
            let _ = tx.send(Event::Message(input));
        }
        let _ = tx.send(Event::EOF);
        Ok::<_, anyhow::Error>(())
    });
    for input in rx {
        node.step(input, &mut stdout)
            .context("Node step fucntion failed")?;
    }
    th.join()
        .expect("Thread join failed")
        .context("stdin process failed")?;
    Ok(())
}
