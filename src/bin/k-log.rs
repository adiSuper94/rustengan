use regex::Regex;
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
    curr_offset: usize,
    logs_to_process: HashMap<usize, LogToProcess>,
}

struct LogToProcess {
    key: String,
    msg: usize,
    request_src: String,
    offset: Option<usize>,
    request_msg_id: Option<usize>,
    cas_msg_id: usize,
    cas_failed: bool,
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
                    let log_details = LogToProcess {
                        key: key.clone(),
                        msg: msg.clone(),
                        request_src: input.src.clone(),
                        offset: Some(self.curr_offset + 1),
                        request_msg_id: input.body.id,
                        cas_msg_id: self.id,
                        cas_failed: false,
                    };
                    self.logs_to_process.insert(self.id, log_details);
                    let payload = Payload::Cas {
                        key: "offset".to_string(),
                        from: self.curr_offset,
                        to: self.curr_offset + 1,
                        put: true,
                    };
                    let mut reply = input.construct_reply(payload, Some(&mut self.id));
                    reply.body.in_reply_to = None;
                    reply.dest = "lin-kv".to_string();
                    self.send(&reply, output)?;
                    self.curr_offset += 1;
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
                Payload::CasOk => {
                    let cas_msg_id = &input.body.in_reply_to.unwrap_or(0);
                    if let Some(log_details) = self.logs_to_process.get(cas_msg_id) {
                        let key = log_details.key.clone();
                        let send_msg_scr = log_details.request_src.clone();
                        let msg = log_details.msg;
                        let log = self.logs.entry(key).or_insert(BTreeMap::new());
                        if let Some(offset) = log_details.offset {
                            log.insert(offset, msg);
                            let payload = Payload::SendOk { offset };
                            let mut send_ok_response =
                                input.construct_reply(payload, Some(&mut self.id));
                            send_ok_response.body.in_reply_to = log_details.request_msg_id;
                            send_ok_response.dest = send_msg_scr;
                            self.send(&send_ok_response, output)?;
                            self.logs_to_process.remove(&cas_msg_id);
                        }
                    }
                }
                Payload::Error { code, text } => match code {
                    20 => {
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
                    22 => {
                        let regex =
                            Regex::new(r"current value (?<new>\d+) is not (?<old>\d+)").unwrap();
                        if let Some(caps) = regex.captures(text) {
                            let new_val = usize::from_str_radix(&caps["new"], 10).unwrap();
                            self.curr_offset = new_val;
                            if let Some(log_details) = self
                                .logs_to_process
                                .get_mut(&input.body.in_reply_to.unwrap_or(usize::MAX))
                            {
                                log_details.cas_failed = true;
                            }
                        }
                    }
                    _ => {}
                },
                Payload::SendOk { .. }
                | Payload::PollOk { .. }
                | Payload::CommitOffsetsOk
                | Payload::ListCommittedOffsetsOk { .. }
                | Payload::Cas { .. } => {}
            },
            Event::InjectedPayload(injected_payload) => match &injected_payload {
                InjectedPayload::Gossip => {
                    for (_key, log_details) in self.logs_to_process.iter_mut() {
                        if !log_details.cas_failed {
                            continue;
                        }
                        let payload = Payload::Cas {
                            key: "offset".to_string(),
                            from: self.curr_offset,
                            to: self.curr_offset + 1,
                            put: true,
                        };
                        let message = Message {
                            src: self.node.to_string(),
                            dest: "lin-kv".to_string(),
                            body: Body {
                                payload,
                                id: Some(self.id),
                                in_reply_to: None,
                            },
                        };
                        log_details.cas_msg_id = self.id;
                        log_details.cas_failed = false;
                        self.id += 1;
                        self.send(&message, output)?;
                        break;
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
                std::thread::sleep(Duration::from_millis(10));
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
            curr_offset: 0,
        };
        Ok(node)
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<KLogNode, _, _>()
}
