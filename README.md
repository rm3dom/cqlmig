# CQL Migrator

Run migrations on a CQL database (Casandra or ScyllaDB).

**This crate is alpha! Use with caution!**

cqlmig simply runs a list of migrations on a database with the following caveats / guarantees:
* Migrations are sorted and run in order as provided.
* Migrations are run only once.
* There are no locks/transactions, be sure to run migrations from a single instance.
* There are no transactions, broken migrations will not be rolled back.
* Batch statements are not supported, mainly because of the sloppy way the file is parsed.
* Calling `migrate` multiple times with out-of-order versions will not produce an error unless the database produces one.
  In other words cqlmig does not error if a previous version is run after a more recent version.
  This scenario normally happens when an older branch was merged with older migrations. 
  Ideally you want to catch this scenario in your PR's, or better have your pipelines fail, rather than panicking at runtime.
  Not trying to justify it ;) happy to add an option flag in for it if your requirements are different.
* Multiline comments are not supported.
* CQL statements must be seperated by a ';' (semicolon) and a '\n' (endline).
* Everything is read into memory and may consume a bit when there are many large migrations.
* File IO is not async.
* And many more .....

 ```rust
extern crate core;
use std::borrow::Borrow;
use std::error::Error;
use std::path::Path;
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cqlmig_cdrs_tokio::{CdrsDbSession, CqlMigrator, DbSession, Migration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(String::from("localhost:9042").into())
        .build()
        .await
        .unwrap();

    let db = TcpSessionBuilder::new(
        RoundRobinLoadBalancingStrategy::new(),
        cluster_config)
        .build()
        .unwrap()
        .borrow()
        .into();

    CqlMigrator::default()
        .with_logger(|s| println!("{}", s))
        .migrate(&db, Migration::from_path(Path::new("/migrations").into()).unwrap())
        .await?;
    Ok(())
}
```

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))
- MIT license ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.

