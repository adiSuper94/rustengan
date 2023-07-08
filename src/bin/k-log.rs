use rustengan::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    io::StdoutLock,
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    Cas {
        key: String,
        from: usize,
        to: usize,
        #[serde(default, rename = "create_if_not_exists")]
        put: bool,
    },
    CasOk,
    Error {
        code: isize,
        text: String,
    },
}

enum InjectedPayload {
    Gossip,
}

struct KLogNode {
    node: String,
    nodes: Vec<String>,
    id: usize,
    logs: HashMap<String, BTreeMap<usize, usize>>,
    processed_till: HashMap<String, usize>,
    logs_to_process: HashMap<String, (Option<usize>, String, usize, Option<usize>)>,
}

impl Node<Payload, InjectedPayload> for KLogNode {
    fn step(
        &mut self,
        event: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match &event {
            Event::Message(input) => match &input.body.payload {
                Payload::Send { key, msg } => {
                    let payload = Payload::Read { key: key.clone() };
                    let mut reply = input.construct_reply(payload, Some(&mut self.id));
                    self.logs_to_process.insert(
                        input.src.clone(),
                        (None, key.clone(), msg.clone(), input.body.id),
                    );
                    reply.body.in_reply_to = None;
                    self.send(&reply, output)?;
                }
                Payload::Poll { offsets } => {
                    let mut msgs: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
                    for (key, offset) in offsets.iter() {
                        let mut msgs_after_offset: Vec<(usize, usize)> = Vec::new();
                        if let Some(log) = self.logs.get(key) {
                            for (log_offset, msg) in log.iter() {
                                if log_offset >= offset {
                                    msgs_after_offset.push((log_offset.clone(), msg.clone()));
                                }
                            }
                            msgs.insert(key.to_string(), msgs_after_offset);
                        }
                    }
                    let reply = input.construct_reply(Payload::PollOk { msgs }, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::CommitOffsets { offsets } => {
                    for (key, offset) in offsets.iter() {
                        if let Some(_log) = self.logs.get(key) {
                            self.processed_till.insert(key.to_string(), offset.clone());
                        }
                    }
                    let reply = input.construct_reply(Payload::CommitOffsetsOk, Some(&mut self.id));
                    self.send(&reply, output)?;
                }
                Payload::ListCommittedOffsets { keys } => {
                    let mut commited_offsets: HashMap<String, usize> = HashMap::new();
                    for key in keys.iter() {
                        if let Some(commited_offset) = self.processed_till.get(key) {
                            commited_offsets.insert(key.to_string(), commited_offset.clone());
                        }
                    }
                    let reply = input.construct_reply(
                        Payload::ListCommittedOffsetsOk {
                            offsets: commited_offsets,
                        },
                        Some(&mut self.id),
                    );
                    self.send(&reply, output)?;
                }
                Payload::ReadOk { value } => {
                    if let Some(entry) = self.logs_to_process.iter_mut().next() {
                        let val = entry.1;
                        let payload = Payload::Cas {
                            key: "offset".to_string(),
                            from: *value,
                            to: *value + 1,
                            put: true,
                        };
                        val.0 = Some(*value + 1);
                        let mut message = input.construct_reply(payload, Some(&mut self.id));
                        message.body.in_reply_to = None;
                        self.send(&message, output)?;
                    }
                }
                Payload::CasOk => {
                    if let Some(entry) = self.logs_to_process.iter().next() {
                        let send_msg_scr = entry.0.clone();
                        let log_details = entry.1.clone();
                        let key = log_details.1;
                        let val = log_details.2;
                        let log = self.logs.entry(key.clone()).or_insert(BTreeMap::new());
                        if let Some(offset) = log_details.0 {
                            log.insert(offset, val);
                            let payload = Payload::SendOk { offset };
                            let mut send_ok_response =
                                input.construct_reply(payload, Some(&mut self.id));
                            send_ok_response.body.in_reply_to = log_details.3;
                            self.logs_to_process.remove(&send_msg_scr);
                            send_ok_response.dest = send_msg_scr;
                            self.send(&send_ok_response, output)?;
                        }
                    }
                }
                Payload::Error { code, text: _text } => {
                    if code == &20 {
                        let payload = Payload::Cas {
                            key: "offset".to_string(),
                            from: 0,
                            to: 0,
                            put: true,
                        };
                        let mut message = input.construct_reply(payload, Some(&mut self.id));
                        message.body.in_reply_to = None;
                        self.send(&message, output)?;
                    }
                }
                Payload::SendOk { .. }
                | Payload::PollOk { .. }
                | Payload::CommitOffsetsOk
                | Payload::ListCommittedOffsetsOk { .. }
                | Payload::Read { .. }
                | Payload::Cas { .. } => {}
            },
            Event::InjectedPayload(injected_payload) => match &injected_payload {
                InjectedPayload::Gossip => {
                    let payload = Payload::Read {
                        key: "offset".to_string(),
                    };
                    if let Some(_dest) = self.logs_to_process.keys().next() {
                        let message = Message {
                            src: self.node.to_string(),
                            dest: "lin-kv".to_string(),
                            body: Body {
                                payload,
                                id: Some(self.id),
                                in_reply_to: None,
                            },
                        };
                        self.id += 1;
                        self.send(&message, output)?;
                    }
                }
            },
            Event::EOF => {}
        }
        Ok(())
    }

    fn from_init(
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        std::thread::spawn(move || {
            // TODO: Handle EOF
            loop {
                std::thread::sleep(Duration::from_millis(100));
                if tx
                    .send(Event::InjectedPayload(InjectedPayload::Gossip))
                    .is_err()
                {
                    break;
                }
            }
        });
        let node = KLogNode {
            id: 1,
            logs: HashMap::new(),
            processed_till: HashMap::new(),
            nodes: init
                .node_ids
                .into_iter()
                .filter(|n| n != &init.node_id)
                .collect(),
            node: init.node_id,
            logs_to_process: HashMap::new(),
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<KLogNode, _, _>()
}
