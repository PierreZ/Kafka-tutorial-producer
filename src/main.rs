mod config;
use config::Settings;
extern crate config as configrs;
#[macro_use]
extern crate serde_derive;
extern crate serde;
#[macro_use]
extern crate structopt;
use std::error::Error;
use structopt::StructOpt;

extern crate futures;
extern crate rdkafka;

extern crate serde_json;

use futures::*;

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

extern crate futures_timer;

use std::time::Duration;

use futures_timer::Interval;

extern crate rand;

use rand::prelude::*;

#[macro_use]
extern crate fake;

// {"email":"estell@gmail.com","company_name":"Heidenreich and Sons","user_name":"patience_et","phone_number":"8033-6763-8232","avatar":"https://robohash.org/khalil0124.png?size=100x100","pack":"pack-medium","name":"Keshaun Koch","profession":"\"human resources\"","premium":false,"credit":16288,"time_zone":"\"Asia/Magadan\"","user_agent":"\"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27\""}
#[derive(Serialize, Deserialize, Debug)]
struct User {
    email: String,
    company_name: String,
    user_name: String,
    phone_number: String,
    avatar: String,
    pack: String,
    name: String,
    profession: String,
    premium: bool,
    credit: i64,
    time_zone: String,
    user_agent: String,
}

fn produce(brokers: &str, topic_name: &str, interval: u64) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let mut rng = thread_rng();

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.

    let _ = Interval::new(Duration::from_micros(interval))
        .for_each(|()| {
            let user = User {
                credit: fake!(Number.between(-10000, 99999)),
                premium: fake!(Boolean.boolean),
                profession: format!("{:?}", fake!(Company.profession)),
                email: fake!(Internet.free_email),
                company_name: fake!(Company.name),
                user_name: fake!(Internet.user_name),
                phone_number: fake!(PhoneNumber.phone_number_with_format("N####-####-####")),
                avatar: fake!(
                    r#"https://robohash.org/{username}.png?size=100x100"#,
                    [username = Internet.user_name]
                ),
                pack: fake!(r#"pack-{size}"#, [size = expr ["small", "medium", "large"][rng.gen_range(0, 3)]]),
                name: fake!(Name.name in en),
                time_zone: format!("{:?}", fake!(Address.time_zone)),
                user_agent: format!("{:?}", fake!(Internet.user_agent)),
            };

            let value = serde_json::to_string(&user).unwrap();

            match producer
                .send_copy(topic_name, None, Some(&value), Some("a"), None, 0)
                .map(move |delivery_status| {
                    // This will be executed onw the result is received
                    delivery_status
                })
                .wait()
            {
                Ok(v) => println!("pushed message at offset {:?}", v.unwrap()),
                Err(e) => println!("error pushing message: {:?}", e),
            };

            Ok(())
        })
        .wait();
}

#[derive(StructOpt, Debug)]
#[structopt(name = "producer")]
struct Opt {
    /// config file
    #[structopt(short = "c", long = "config")]
    config: String,
}

fn main() -> Result<(), Box<Error>> {
    let opt = Opt::from_args();
    let settings = Settings::from(opt.config)?;

    produce(&settings.brokers, &settings.topic, settings.interval);

    Ok(())
}
