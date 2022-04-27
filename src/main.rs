mod memkv;

use futures::future::join_all;
use log::{info, warn};
use log4rs;
use memmap::MmapMut;
use serde::{Deserialize, Serialize};
use std::error;
use std::fs;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::path::Path;
use std::rc::Rc;
use std::string::ToString;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use uuid::Uuid;

fn load_seeds(file: &str) -> Vec<SocketAddr> {
    let contents = fs::read_to_string(file).expect("Failed to load node config");
    return contents
        .split("\n")
        .filter_map(|s| s.parse().ok())
        .collect();
}

fn print_addresses(addresses: Vec<SocketAddr>) {
    println!(
        "{}",
        addresses
            .iter()
            .map(|ip_addr| ip_addr.to_string())
            .collect::<Vec<String>>()
            .join(";")
    )
}

#[derive(Serialize, Deserialize)]
struct Person {
    name: String,
    age: u8,
    phones: Vec<String>,
}

struct Message {
    key: String,
    value: memkv::Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let (tx, rx) = mpsc::channel::<Message>();

    let (key_tx, key_rx) = mpsc::channel::<&str>();

    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();
    // let mut handles = vec![];
    let _node_id = Uuid::new_v4();

    let mut seeds = load_seeds("config/seeds");
    if Path::new("keyspace").exists() {
        let _return = fs::remove_file("keyspace");
    }

    let mut kvmap = memkv::MemKvPage::new(Path::new("keyspace"))?;

    thread::spawn(move || {
        let vals = vec![
            String::from("apple"),
            String::from("banana"),
            String::from("organe"),
            String::from("strawberry"),
        ];

        for val in vals {
            tx.send(Message {
                key: val.clone(),
                value: memkv::Value::String(val.clone().to_uppercase()),
            })
            .unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    });

    let mut sign = Arc::new(kvmap);

    thread::spawn(move || loop {
        for key in &key_rx {
            println!("test {:?}", sign.get(&key));
        }
        thread::sleep(Duration::from_secs(1));
    });

    loop {
        for received in &rx {
            println!("Got: {}", received.key);

            sign.insert(&received.key, received.value)?;
            key_tx.send(&received.key)?;
        }
        thread::sleep(Duration::from_secs(1));
    }

    /*kvmap.insert("albert", kv_mmap::Value::String(String::from("value")))?;
    kvmap.insert("peter", kv_mmap::Value::Integer(123))?;
    kvmap
        .insert(
            "tom",
            kv_mmap::Value::String(String::from("my third value")),
        )
        .unwrap();
    let person_a = Person {
        name: String::from("peter pan"),
        age: 20,
        phones: vec![],
    };
    kvmap.insert("dan", kv_mmap::Value::Blob(person_a))?;
    println!("{}", seeds.len());*/
    return Ok(());
    /* let (sender, mut node_seed_receiver) = watch::channel(vec![]);

    handles.push(tokio::spawn(async move {
        while node_seed_receiver.changed().await.is_ok() {
            println!("received new nodes = {:?}", *node_seed_receiver.borrow());
        }
    }));

    sender.send(seeds.clone()).expect("failed to send");
    sleep(Duration::from_millis(100)).await;
    seeds.append(&mut vec!["127.0.0.1:2002".parse().expect("Failed")]);
    sender.send(seeds.clone()).expect("failed to send");

    futures::future::join_all(handles).await;
    */
}
