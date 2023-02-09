extern crate core;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use async_trait::async_trait;
use regex::{Match, Regex};
use ring::digest::{Context, SHA256};

const SELECT_MIGRATIONS: &str = "select version, description, status, shasum
from migration.migrations;";

const SELECT_SCHEMA_TABLE: &str = "select table_name
from system_schema.tables
where
   keyspace_name = 'migration'
   and table_name = 'migrations';";

const UPDATE_MIGRATION: &str = "update migration.migrations
set
    applied = toTimestamp(now()),
    description = '{description}',
    shasum = '{shasum}',
    status = {status},
    status_text = '{status_text}'
where version = '{version}';";

pub type GenResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// An abstraction around a database row in the simplest form.
///
/// This should be implemented by a DB provider.
pub trait DbRow {
    fn string_by_name(&self, name: &str, def: String) -> GenResult<String>;
    fn i32_by_name(&self, name: &str, def: i32) -> GenResult<i32>;
}

/// An abstraction around a database session / connection in the simplest form.
///
/// This should be implemented by a DB provider.
#[async_trait]
pub trait Db: Sync {
    type Row: DbRow;
    async fn query(&self, query: &str) -> GenResult<Vec<Self::Row>>;
    async fn query_param(
        &self,
        query: &str,
        params: HashMap<String, String>,
    ) -> GenResult<Vec<Self::Row>> {
        let q = params.iter().fold(query.to_string(), |q, p| {
            q.replace(format!("{{{}}}", p.0).as_str(), p.1.as_str())
        });
        self.query(q.as_str()).await
    }
}

/// General migration error.
#[derive(Debug, Clone)]
pub struct MigrationError {
    message: String,
}

impl Display for MigrationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let res = write!(f, "({})", self.message);
        res
    }
}

impl Error for MigrationError {}

impl MigrationError {
    pub fn new(s: &str) -> Self {
        Self {
            message: s.to_string(),
        }
    }
}

/// Migration version.
///
/// Version numbers / file names can be be in the following formats where a double '__' must be used to separate the description:
/// * (V?)(major)(_minor)?(_patch)?(__description)?
/// * (major)(.minor)?(.patch)?(__description)?
///
/// # Examples
/// ```
/// use std::str::FromStr;
/// use cqlmig::Version;
///
/// # let max = Version::from_str("1.2.4").unwrap();
/// # let eq = Version::from_str("1.2.3").unwrap();
/// // Valid
/// let v = Version::from_str("1.2.3__Some Description").unwrap();
/// # assert_eq!(1, v.major);
/// # assert_eq!(2, v.minor);
/// # assert_eq!(3, v.patch);
/// # assert_eq!(Some("Some Description".to_string()), v.description);
/// # assert!(v < max);
/// # assert!(max > v);
/// # assert_eq!(eq, v);
/// let v = Version::from_str("V1_2_3__Some Description").unwrap();
/// # assert_eq!(1, v.major);
/// # assert_eq!(2, v.minor);
/// # assert_eq!(3, v.patch);
/// # assert_eq!(Some("Some Description".to_string()), v.description);
/// # assert!(v < max);
/// # assert!(max > v);
/// # assert_eq!(eq, v);
/// let v = Version::from_str("1__4").unwrap();
/// # assert_eq!(1, v.major);
/// # assert_eq!(0, v.minor);
/// # assert_eq!(0, v.patch);
/// # assert_eq!(Some("4".to_string()), v.description);
/// # assert!(v < max);
/// let v = Version::from_str("1").unwrap();
/// # assert_eq!(1, v.major);
/// # assert_eq!(0, v.minor);
/// # assert_eq!(0, v.patch);
/// # assert_eq!(None, v.description);
/// # assert!(v < max);
///
/// // Error
/// let _ = Version::from_str("12.__4").unwrap_err();
/// let _ = Version::from_str("12.__").unwrap_err();
/// let _ = Version::from_str("__").unwrap_err();
/// let _ = Version::from_str("1__").unwrap_err();
/// ```
#[derive(Debug, Clone, Default)]
pub struct Version {
    pub major: i32,
    pub minor: i32,
    pub patch: i32,
    pub description: Option<String>,
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let desc = self.description.as_ref().cloned().unwrap_or_default();
        let res = write!(f, "{}.{}.{} {}", self.major, self.minor, self.patch, desc);
        res
    }
}

impl PartialEq<Self> for Version {
    fn eq(&self, other: &Self) -> bool {
        self.major.eq(&other.major) && self.patch == other.patch && self.minor == other.minor
    }
}

impl Eq for Version {}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.major > other.major {
            return Ordering::Greater;
        }
        if self.major < other.major {
            return Ordering::Less;
        }
        if self.minor > other.minor {
            return Ordering::Greater;
        }
        if self.minor < other.minor {
            return Ordering::Less;
        }
        if self.patch > other.patch {
            return Ordering::Greater;
        }
        if self.patch < other.patch {
            return Ordering::Less;
        }
        Ordering::Equal
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }

    fn lt(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Less
    }

    fn le(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Less || self.cmp(other) == Ordering::Equal
    }

    fn gt(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Greater
    }

    fn ge(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Greater || self.cmp(other) == Ordering::Equal
    }
}

impl FromStr for Version {
    type Err = MigrationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn as_ver(cap: Option<Match>) -> i32 {
            i32::from_str(cap.map_or("0", |m| m.as_str())).unwrap_or_default()
        }

        fn as_str(cap: Option<Match>) -> Option<String> {
            cap.map(|m| m.as_str().to_string())
        }

        let err = MigrationError::new(format!("Invalid version format: {}", s).as_str());
        let re = Regex::new(r"^[Vv]?([0-9]+)([._]([0-9]+))?([._]([0-9]+))?((__)(.+))?$").unwrap();
        let caps = re.captures(s).ok_or(err)?;
        let v = Self {
            major: as_ver(caps.get(1)),
            minor: as_ver(caps.get(3)),
            patch: as_ver(caps.get(5)),
            description: as_str(caps.get(8)),
        };
        Ok(v)
    }
}

impl Version {
    /// Convert to a semantic version formatted string.
    ///
    /// ```
    /// use std::str::FromStr;
    /// use cqlmig::Version;
    ///
    /// let v = Version::from_str("1_2_3__Some Description").unwrap().to_semver();
    /// assert_eq!("1.2.3", v);
    /// ```
    pub fn to_semver(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }

    /// Gets the description or empty string if there is no description.
    ///
    /// ```
    /// use std::str::FromStr;
    /// use cqlmig::Version;
    ///
    /// let d = Version::from_str("1_2_3__Some Description").unwrap().description();
    /// assert_eq!("Some Description", d);
    /// ```
    pub fn description(&self) -> String {
        self.description.as_ref().cloned().unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq)]
enum MigrationStatus {
    NA,
    Ok,
    Error,
}

/// A migration to be applied.
#[derive(Debug, Clone)]
pub struct Migration {
    version: Version,
    status: MigrationStatus,
    shasum: String,
    statements: Vec<String>,
}

impl Migration {
    fn is_successful(&self) -> bool {
        self.status == MigrationStatus::Ok
    }

    /// Statements part of this migration.
    pub fn statements(&self) -> &Vec<String> {
        &self.statements
    }

    /// The version number.
    pub fn version(&self) -> &Version {
        &self.version
    }

    /// Gets the shasum, equivalent to GNU coreutils sha256sum.
    ///
    /// shasum is only calculated for files / bytes.
    pub fn shasum(&self) -> &String {
        &self.shasum
    }

    /// Create a new [`Migration`].
    ///
    /// See [`Version`] for `version` format.
    ///
    /// ```
    /// use cqlmig::Migration;
    ///
    /// let _ = Migration::new("0.0.1", vec![
    ///     "CREATE TABLE IF NOT EXISTS examples.example ( key text PRIMARY KEY)"
    /// ]);
    /// ```
    pub fn new(version: &str, statements: Vec<&str>) -> GenResult<Self> {
        Ok(Self {
            version: Version::from_str(version)?,
            statements: statements.iter().map(|s| s.to_string()).collect(),
            status: MigrationStatus::NA,
            shasum: String::new(),
        })
    }

    fn from_reader<R: Read>(version: &str, shasum: &str, reader: R) -> GenResult<Self> {
        let mut reader = BufReader::new(reader);
        let mut migration = Self {
            version: Version::from_str(version)?,
            status: MigrationStatus::NA,
            shasum: shasum.to_string(),
            statements: vec![],
        };

        let mut stmt = String::new();

        loop {
            let mut line = String::new();
            let res = reader.read_line(&mut line);
            match res {
                Ok(0) => break,
                Err(e) => return Err(e.into()),
                Ok(_) => {}
            }
            let x = line.trim();

            if x.starts_with("/*") {
                return Err(MigrationError::new("Multiline comments not supported").into());
            }

            if !x.is_empty() && !x.starts_with("--") && !x.starts_with("//") {
                stmt.push_str(line.as_str());
                if x.ends_with(';') {
                    migration.statements.push(stmt);
                    stmt = String::new()
                }
            }
        }

        if !stmt.is_empty() {
            migration.statements.push(stmt);
        }

        Ok(migration)
    }

    /// Create a new [`Migration`].
    ///
    /// See [`Version`] for `version` format.
    ///
    /// ```ignore
    /// use cqlmig::Migration;
    ///
    /// let _ = Migration::from_bytes("0_0_1__Parties Init",
    ///             include_bytes!("migrations/V0_0_1__Parties Init.cql"));
    /// ```
    pub fn from_bytes(version: &str, bytes: &[u8]) -> GenResult<Self> {
        let shasum = sha256_digest64(bytes)?;
        Migration::from_reader(version, shasum.as_str(), bytes)
    }

    /// Create a new [`Migration`].
    ///
    /// See [`Version`] for `version` format.
    ///
    /// ```
    /// use std::path::Path;
    /// use cqlmig::Migration;
    ///
    /// # static SHASUM: &str = "e24e86bf84c256077c327bdb23e33b440c08dbde2e3b7f46b744b0b87f43f5a2";
    /// let m = Migration::from_file(Path::new("src/migrations/V0_0_0__Init cqlmig.cql").into())
    /// .unwrap();
    /// # let s = m.statements();
    /// # assert_eq!(2, s.len());
    /// # assert!(s[0].starts_with("CREATE KEYSPACE"));
    /// # assert!(s[1].starts_with("CREATE TABLE"));
    /// # assert_eq!(SHASUM, m.shasum());
    /// ```
    pub fn from_file(path: Box<Path>) -> GenResult<Self> {
        if !path.is_file() {
            return Err(MigrationError::new("Not a file").into());
        }

        let fname = path
            .file_name()
            .map(|s| s.to_str())
            .unwrap_or_default()
            .unwrap_or_default();

        let v = &fname.replace(".cql", "");
        let c = sha256_digest64(File::open(&path)?)?;

        Migration::from_reader(v.as_str(), c.as_str(), File::open(&path)?)
    }

    /// Read [`Migration`]s from path.
    ///
    /// See [`Version`] for `version` format.
    ///
    ///
    /// ```
    /// use std::path::Path;
    /// use cqlmig::Migration;
    /// # static SHASUM: &str = "e24e86bf84c256077c327bdb23e33b440c08dbde2e3b7f46b744b0b87f43f5a2";
    /// let v1 = Migration::from_path(Path::new("src/migrations").into()).unwrap();
    /// # assert_eq!(1, v1.len());
    /// let v2 = Migration::from_path(Path::new("src/migrations/V0_0_0__Init cqlmig.cql").into())
    /// .unwrap();
    /// # assert_eq!(1, v2.len());
    /// # assert_eq!(v1[0].shasum(), v2[0].shasum());
    /// # assert_eq!(SHASUM, v1[0].shasum());
    /// ```
    pub fn from_path(path: Box<Path>) -> GenResult<Vec<Migration>> {
        use std::fs;

        let md = fs::metadata(&path)?;

        let files = match md.is_dir() {
            true => {
                let paths = fs::read_dir(&path)?;
                let mut cql_files: Vec<PathBuf> = vec![];
                for path in paths {
                    let de = path?;
                    let ft = de.file_type()?;
                    if ft.is_dir() {
                        continue;
                    }
                    let name = de.file_name().to_str().unwrap_or_default().to_string();
                    if name.ends_with(".cql") {
                        cql_files.push(de.path());
                    }
                }
                cql_files
            }
            false => {
                if path.to_str().unwrap_or_default().ends_with(".cql") {
                    vec![path.canonicalize()?]
                } else {
                    vec![]
                }
            }
        };

        if files.is_empty() {
            return Err(MigrationError::new("No files found to migrate").into());
        }

        let mut migrations: Vec<Migration> = vec![];
        for p in files {
            migrations.push(Migration::from_file(p.into_boxed_path())?);
        }

        Ok(migrations)
    }
}

impl PartialEq<Self> for Migration {
    fn eq(&self, other: &Self) -> bool {
        self.version.eq(&other.version)
    }
}

impl Eq for Migration {}

impl PartialOrd for Migration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.version.partial_cmp(&other.version)
    }

    fn lt(&self, other: &Self) -> bool {
        self.version.lt(&other.version)
    }

    fn le(&self, other: &Self) -> bool {
        self.version.le(&other.version)
    }

    fn gt(&self, other: &Self) -> bool {
        self.version.gt(&other.version)
    }

    fn ge(&self, other: &Self) -> bool {
        self.version.ge(&other.version)
    }
}

impl Ord for Migration {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Less)
    }
}

// Equivalent to GNU coreutils sha256sum
fn sha256_digest64<R: Read>(mut reader: R) -> GenResult<String> {
    fn to_hex(bytes: &[u8]) -> String {
        let mut s = String::new();
        for &byte in bytes {
            write!(&mut s, "{:02x}", byte).expect("Unable to write");
        }
        s
    }

    let mut context = Context::new(&SHA256);
    let mut buffer = [0; 1024];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

    let ctx = context.finish();
    let mut hex = to_hex(ctx.as_ref());
    hex.truncate(64);
    Ok(hex)
}

async fn get_applied(db: &impl Db) -> GenResult<Vec<Migration>> {
    let applied_rows = db.query(SELECT_MIGRATIONS).await?;

    let mut applied: Vec<Migration> = vec![];
    for row in applied_rows {
        let v = Version::from_str(row.string_by_name("version", String::new())?.as_str())?;

        let d = row.string_by_name("description", String::new())?;
        applied.push(Migration {
            version: Version {
                description: Some(d),
                ..v
            },
            status: match row.i32_by_name("status", 1)? {
                -1 => MigrationStatus::NA,
                0 => MigrationStatus::Ok,
                _ => MigrationStatus::Error,
            },
            shasum: row.string_by_name("shasum", String::new())?,
            statements: vec![],
        })
    }
    applied.sort();
    Ok(applied)
}

async fn apply(db: &impl Db, migrate: &Migration) -> GenResult<()> {
    for s in &migrate.statements {
        let _ = db.query(s).await?;
    }
    Ok(())
}

async fn apply_with_update(
    db: &impl Db,
    migrate: &Migration,
    fail_on_err: bool,
    log: fn(String) -> (),
) -> GenResult<()> {
    let res = apply(db, migrate).await;
    if res.is_ok() {
        log(format!(
            "Successfully applied migration: {}",
            migrate.version
        ))
    }
    if res.is_err() {
        log(format!(
            "Failed applying migration: {} with error: {}",
            migrate.version,
            res.as_ref().unwrap_err()
        ));
        if fail_on_err {
            return Err(res.unwrap_err());
        }
    }
    let _ = db
        .query_param(
            UPDATE_MIGRATION,
            HashMap::from([
                ("description".to_string(), migrate.version.description()),
                ("version".to_string(), migrate.version.to_semver()),
                ("shasum".to_string(), migrate.shasum.to_string()),
                (
                    "status".to_string(),
                    (if res.is_ok() { 0 } else { 1 }).to_string(),
                ),
                (
                    "status_text".to_string(),
                    match &res {
                        Ok(_) => "ok".to_string(),
                        Err(e) => e.to_string(),
                    },
                ),
            ]),
        )
        .await?;
    res
}

// Determine if a migration is in the list of applied migrations and was successful.
fn was_applied(applied: &[Migration], m: &Migration) -> bool {
    applied
        .iter()
        .any(|it| it.version.eq(&m.version) && it.is_successful())
}

// Called every time before running migrations.
// Runs own migrations if required and returns a list of previously applied migrations.
async fn setup(db: &impl Db, log: fn(String) -> ()) -> GenResult<Vec<Migration>> {
    //Query to see if the schema table exists, if it does get the list of applied migrations.
    let res = db.query(SELECT_SCHEMA_TABLE).await?;
    let applied = match res.len() {
        0 => vec![],
        _ => get_applied(db).await?,
    };

    let migrations = migrations();
    for m in migrations {
        //Its applied and successful
        if was_applied(&applied, &m) {
            continue;
        }
        // Else apply
        apply_with_update(db, &m, true, log).await?;
    }
    Ok(applied)
}

/// Entrypoint / Builder for running migrations.
///
/// [`CqlMigrator`] simply runs a list of [`Migration`]'s on a database with the following caveats / guarantees:
/// * Migrations are sorted and run in order as provided.
/// * Migrations are run only once.
/// * Calling `migrate` multiple times with out of order versions will not produce an error.
/// * Multiline comments are not supported.
/// * CQL statements must be seperated by a ';' semicolon.
///
/// # Examples
///
/// ```ignore
/// use std::path::Path;
/// use cqlmig::{CqlMigrator, Migration};
///
/// let ses = CdrsDbSession::connect_no_auth(vec!["localhost:9042"]).await.unwrap();
/// let db: CdrsDbSession = ses.borrow().into();
/// CqlMigrator::default()
///   .with_logger(|s| println!("{}", s))
///   .migrate(&db, Migration::from_path(Path::new("src/migrations").into()).unwrap())
///   .await.unwrap();
/// ```
#[derive(Clone)]
pub struct CqlMigrator {
    log: fn(String) -> (),
    continue_on_error: bool,
}

impl CqlMigrator {
    /// Add a logging function.
    ///
    /// See [`CqlMigrator`] for an example.
    pub fn with_logger(self, log: fn(String) -> ()) -> Self {
        CqlMigrator { log, ..self }
    }

    /// If true, continue to run all migrations; default false.
    pub fn continue_on_error(self, b: bool) -> Self {
        CqlMigrator {
            continue_on_error: b,
            ..self
        }
    }

    /// Runs a list of provided migrations.
    ///
    /// See [`CqlMigrator`] for an example.
    pub async fn migrate(&self, db: &impl Db, mut migrations: Vec<Migration>) -> GenResult<()> {
        let applied = setup(db, self.log).await?;
        migrations.sort();
        for m in migrations {
            if was_applied(&applied, &m) {
                continue;
            }
            apply_with_update(db, &m, !self.continue_on_error, self.log).await?
        }
        Ok(())
    }

    /// Run migrations in a folder or file.
    ///
    /// See [`CqlMigrator`] for an example.
    pub async fn migrate_files(&self, db: &impl Db, path: Box<Path>) -> GenResult<()> {
        self.migrate(db, Migration::from_path(path)?).await
    }
}

impl Default for CqlMigrator {
    fn default() -> Self {
        Self {
            log: |_| {},
            continue_on_error: false,
        }
    }
}

// init cqlmig table
fn migrations() -> Vec<Migration> {
    vec![Migration::from_bytes(
        "0.0.0__Init cqlmig",
        include_bytes!("migrations/V0_0_0__Init cqlmig.cql"),
    )
    .unwrap()]
}

#[cfg(test)]
mod tests {
    use crate::{Migration};

    static SHASUM: &str = "e24e86bf84c256077c327bdb23e33b440c08dbde2e3b7f46b744b0b87f43f5a2";

    #[test]
    fn test_from_bytes() {
        let m = Migration::from_bytes(
            "0.0.0",
            include_bytes!("migrations/V0_0_0__Init cqlmig.cql"),
        )
        .unwrap();
        assert_eq!(2, m.statements.len());
        assert!(m.statements[0].starts_with("CREATE KEYSPACE"));
        assert!(m.statements[1].starts_with("CREATE TABLE"));
        assert_eq!(SHASUM, m.shasum);
    }
}
