use async_trait::async_trait;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::ByName;

pub use cqlmig::{CqlMigrator, GenResult, Migration};
use cqlmig::{Db, DbRow};

/// A db session.
pub type DbSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

impl<'a> From<&'a DbSession> for CdrsDbSession<'a> {
    fn from(db: &'a DbSession) -> Self {
        CdrsDbSession::new(db)
    }
}

/// Session wrapper.
///
/// To create a session call `.into()` on a [`DbSession`].
///
/// # Examples
///
/// ```
/// # use std::error::Error;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn Error>> {
/// use std::borrow::Borrow;
/// use cdrs_tokio::cluster::NodeTcpConfigBuilder;
/// use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
/// use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
/// use cqlmig::CqlMigrator;
/// use cqlmig_cdrs_tokio::CdrsDbSession;
///
/// let cluster_config = NodeTcpConfigBuilder::new()
///     .with_contact_points(vec!["localhost:9042".to_string().into()])
///     .build()
///     .await
///     .unwrap();
/// let tcp_ses = TcpSessionBuilder::new(
///      RoundRobinLoadBalancingStrategy::new(),
///      cluster_config)
///  .build()
///  .unwrap();
///
/// let ses: CdrsDbSession = tcp_ses.borrow().into();
///
/// CqlMigrator::default().migrate(&ses, vec![]).await.unwrap();
/// # Ok(())
/// # }
/// ```
pub struct CdrsDbSession<'a> {
    ses: &'a DbSession,
}

impl<'a> CdrsDbSession<'a> {
    /// Helper function to create an un-authenticated connection to an DB.
    ///
    /// See [`CdrsDbSession`] for an example of creating a custom connection.
    pub async fn connect_no_auth(addrs: Vec<String>) -> GenResult<DbSession> {
        let cluster_config = NodeTcpConfigBuilder::new()
            .with_contact_points(addrs.iter().map(|it| it.into()).collect())
            .build()
            .await?;
        let ses = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
            .build()?;
        Ok(ses)
    }

    /// Create a [`CdrsDbSession`].
    pub fn new(db: &'a DbSession) -> CdrsDbSession<'a> {
        CdrsDbSession {
            ses: db,
        }
    }
}

/// Row wrapper.
#[derive(Clone, Debug)]
pub struct ARow {
    row: Row,
}

impl DbRow for ARow {
    fn string_by_name(&self, name: &str, def: String) -> GenResult<String> {
        Ok(self.row.by_name(name)?.unwrap_or(def))
    }

    fn i32_by_name(&self, name: &str, def: i32) -> GenResult<i32> {
        Ok(self.row.by_name(name)?.unwrap_or(def))
    }
}

#[async_trait]
impl<'a> Db for CdrsDbSession<'a> {
    type Row = ARow;

    async fn query(&self, query: &str) -> GenResult<Vec<Self::Row>> {
        let rows = self
            .ses
            .query(query)
            .await?
            .response_body()?
            .into_rows()
            .unwrap_or_default();

        let mut res: Vec<Self::Row> = vec![];

        for row in rows {
            res.push(ARow { row });
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::future::Future;

    use dockertest::Composition;
    use dockertest::DockerTest;

    use cqlmig::{CqlMigrator, GenResult, Migration};

    use crate::CdrsDbSession;

    #[tokio::test]
    async fn test_migrations_with_server() {
        test_migrations(String::from("localhost:9042"))
            .await
            .unwrap();
    }

    async fn test_migrations(addr: String) -> GenResult<()> {
        async fn run(addr: String) -> GenResult<()> {
            let ses = CdrsDbSession::connect_no_auth(vec![addr]).await?;
            let db: CdrsDbSession = ses.borrow().into();
            CqlMigrator::default()
                .with_logger(|s| println!("{}", s))
                .migrate(&db, Vec::<Migration>::new())
                .await
        }

        let _ = run(addr.clone()).await?;
        // Run twice to make sure it does not apply the same migrations twice
        let _ = run(addr.clone()).await?;
        Ok(())
    }

    //For local testing
    //#[tokio::test]
    async fn test_migrations_with_docker() {
        run_with_docker(|addr| async move { test_migrations(addr).await }).await;
    }

    // For local testing
    async fn run_with_docker<T, Fut>(fn_test: T)
    where
        T: FnOnce(String) -> Fut + Send + 'static,
        Fut: Future<Output = GenResult<()>> + Send + 'static,
    {
        let mut dt = DockerTest::new();
        let scylladb = Composition::with_repository("scylladb/scylla")
            .publish_all_ports()
            .clone();

        dt.add_composition(scylladb);
        let _ = dt
            .run_async(|ops| async move {
                // A handle to operate on the Container.
                let container = ops.handle("scylladb/scylla");
                // The container is in a running state at this point.
                let host_port = container.host_port(9042);
                let bind = match host_port {
                    None => panic!("scylladb port not found"),
                    Some(t) => t,
                };

                let _ = fn_test(format!("{}:{}", bind.0, bind.1)).await.unwrap();
            })
            .await;
    }
}
