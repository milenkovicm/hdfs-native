#![allow(dead_code)]

use once_cell::sync::Lazy;
use std::sync::{Arc, Barrier};
use testcontainers::{clients, Container};
use testcontainers_minidfs_rs::MiniDFS;

/// where to expect mini dfs instance
static NAME_NODE_ADDRESS: &str = "localhost";

/// Should MiniDFS be started before tests.
/// Ideally we should detect if there is MiniDFS running and
/// decide if start is needed. This is to make tests faster
/// as MiniDFS needs some time to startup.
///
#[cfg(feature = "tests_start_docker")]
static MINI_DFS_START: bool = true;
#[cfg(not(feature = "tests_start_docker"))]
static MINI_DFS_START: bool = false;

/// Test data (not great, not terrible :))
pub const DATA: &str = "1234567890ABCDEF";

/// Blocks docker thread until shut down is called
static BARRIER_SHUTDOWN: Lazy<Arc<Barrier>> = Lazy::new(|| Arc::new(Barrier::new(2)));
/// Blocks shutdown until docker has been stopped
static BARRIER_SHUTDOWN_CONFIRM: Lazy<Arc<Barrier>> = Lazy::new(|| Arc::new(Barrier::new(2)));

///
/// This method will be started before each integration test file
/// and setup all required parameters of the test. Also, it will start
/// required infrastructure as well, in this case its a MiniDFs (HDFS) cluster,
/// packed as a docker container.
///
/// Short summary of the logic:
///
/// Initialize method will start a new thread which will start docker (test)container,
/// this was required as [Docker] is not sharable between threads which was required
/// to have it as a static property. Thread should be active for duration of tests.
/// [shutdown()] will signal end of the test case, which will trigger two barriers,
/// one to which will allow docker to proceed with shutdown and other which will
/// signal docker shutdown finished and allow [shutdown()] to finish the method.
///
/// To manually run minidfs docker container run:
///
/// ```bash
/// docker run -ti -p 9000:9000 -p 8020:8020 -p 50010:50010 -p 50011:50011 -p 50012:50012 -p 50013:50013 -p 50014:50014 --rm milenkovicm/testcontainer-hdfs
/// ```
///
/// Similar command would be executed by `testcontainer` as well.
///

#[ctor::ctor]
fn initialize() {
    setup_logger();
    setup_environment();
    let _barrier_wait_for_docker_start = Arc::new(Barrier::new(2));
    let barrier_wait_for_docker_start = _barrier_wait_for_docker_start.clone();

    // the thread will start HDFS if needed and keep it running for all the test in test-case
    std::thread::spawn(move || {
        let docker = clients::Cli::default();
        let instance = setup_docker(&docker);
        log::info!("Docker control thread - Started!");
        _barrier_wait_for_docker_start.wait();
        BARRIER_SHUTDOWN.clone().wait();
        match instance {
            Some(instance) => instance.stop(),
            _ => (),
        };
        BARRIER_SHUTDOWN_CONFIRM.clone().wait();
        log::info!("Docker control thread - Shutting down!");
    });

    // Blocks test execution until MiniDFS has been started
    //
    // ( we could remove this to test robustness of the library,
    // and see if the operations will retry )
    barrier_wait_for_docker_start.wait();
    log::info!("Init process finished ...");
}

/// shutdown logic
#[ctor::dtor]
fn shutdown() {
    log::info!("Shuting tests down ...");
    // triggers docker thread to start shutdown process
    BARRIER_SHUTDOWN.clone().wait();
    // waits for docker to finish shutdown process
    BARRIER_SHUTDOWN_CONFIRM.clone().wait();
    log::info!("Shuting tests down ... DONE");
}

/// helper method to startup docker container
fn setup_docker<'c>(docker: &'c clients::Cli) -> Option<Container<'c, MiniDFS>> {
    log::info!("Setting up test environment ... DONE");
    if MINI_DFS_START {
        // prevent of panic during container start.
        // fail will block the test execution as barrier will never
        // get all required participants.
        //
        // if docker start fails there is going to be a dangling container
        // in status `Created` which should be manually cleaned up
        let container = std::panic::catch_unwind(|| docker.run(MiniDFS::runnable())).ok();
        if container.is_some() {
            log::info!("Starting up Mini DFS ... DONE");
        } else {
            log::error!("Starting up Mini DFS ... FAILED")
        }

        container
    } else {
        log::warn!("Automatic Mini DFS is disabled, will not be started.");
        None
    }
}

/// It setups env logger for tests
fn setup_logger() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_filters("hdfs-native=debug,hdfs_native_object_store=debug")
        .is_test(true)
        .try_init();
}

/// It sets environment variable needed for `libhdfs3`
///
/// ```bash
/// export LIBHDFS3_CONF=/Users/gooduser/git/project_dir/libhdfs3-hdfs-client.xml
/// ```
fn setup_environment() {
    std::env::set_var("LIBHDFS3_CONF", "libhdfs3-hdfs-client.xml")
}

pub fn generate_unique_name() -> String {
    rusty_ulid::generate_ulid_string()
}

pub fn generate_hdfs_url() -> String {
    std::env::var("TEST_NAME_NODE_ADDRESS").unwrap_or(format!(
        "hdfs://{}:{}",
        NAME_NODE_ADDRESS,
        testcontainers_minidfs_rs::PORT_NAME_NODE
    ))
}
