use hdfs_native::HdfsRegistry;
use log::info;

fn main() {
    std::env::set_var("LIBHDFS3_CONF", "libhdfs3-hdfs-client.xml");
    const DATA: &str = "1234567890ABCDEF";
    let hdfs_server_url = "hdfs://localhost:9000";
    let test_dir = format!("/{}", "test_dir");
    let test_file = format!("{}/{}", test_dir, "test.file");

    let fs_registry = HdfsRegistry::new();
    info!("HDFS Name Node to be used: [{}]", hdfs_server_url);
    let fs = fs_registry
        .get(&hdfs_server_url)
        .expect("creation of registry");

    info!("Directory used for this tests: [{}]", test_dir);

    fs.mkdir(&test_dir).expect("root dir created");
    assert!(fs.exist(&test_dir));

    info!("File used for basic file operations: [{}]", test_file);

    let mut f = fs.create(&test_file).expect("file open for writing");
    assert!(f.is_writable());
    assert!(!f.is_readable());

    f.write(DATA.as_bytes()).expect("data to be written");
    f.flush();
    drop(f);

    let mut f = fs.append(&test_file).expect("file open for append");
    assert!(f.is_writable());
    assert!(!f.is_readable());
    f.write(DATA.as_bytes()).expect("data to be appended");
    f.flush();

    let f = fs.open(&test_file).expect("file open");
    assert!(!f.is_writable());
    assert!(f.is_readable());

    let mut buf = vec![0; 4 * DATA.len()];

    let len = f.read(&mut buf).expect("data to be read");

    let result = &buf[..len];
    assert_eq!(format!("{}{}", DATA, DATA).as_bytes(), result);

    fs.delete(&test_dir, true).expect("directory to be deleted");
}
