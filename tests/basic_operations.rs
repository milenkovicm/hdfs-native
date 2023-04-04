mod common;

#[cfg(test)]
mod e2e {

    use crate::common::*;
    use hdfs_native::HdfsRegistry;
    use log::info;

    const DATA: &str = "1234567890";

    #[test]
    fn should_connect() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS Name Node to be used: [{}]", hdfs_server_url);
        let _fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");
    }

    #[test]
    fn basic_operations_check() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS Name Node to be used: [{}]", hdfs_server_url);
        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let test_dir = format!("/{}", generate_unique_name());
        info!("Directory used for this tests: [{}]", test_dir);

        fs.mkdir(&test_dir).expect("root dir created");
        assert!(fs.exist(&test_dir));

        let test_file = format!("{}/{}", test_dir, generate_unique_name());
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

    #[test]
    fn read_with_position_check() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS Name node to be used: [{}]", hdfs_server_url);

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let test_dir = format!("/{}", generate_unique_name());
        fs.mkdir(&test_dir).expect("root dir created");
        assert!(fs.exist(&test_dir));

        let test_file = format!("{}/{}", test_dir, generate_unique_name());

        let mut f = fs.create(&test_file).expect("file open for writing");

        f.write(DATA.as_bytes()).expect("data to be written");
        f.flush();

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let f = fs.open(&test_file).expect("file open");

        let mut buf = vec![0; 64];
        let pos = 2;
        let read = f.read_with_pos(pos, &mut buf).expect("read with position ");

        assert_eq!(DATA.len(), read + (pos as usize));
        fs.delete(&test_dir, true).expect("directory to be deleted");
    }

    #[test]
    fn read_with_position_len_check() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS Name node to be used: [{}]", hdfs_server_url);

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let test_dir = format!("/{}", generate_unique_name());
        fs.mkdir(&test_dir).expect("root dir created");
        assert!(fs.exist(&test_dir));

        let test_file = format!("{}/{}", test_dir, generate_unique_name());

        let mut f = fs.create(&test_file).expect("file open for writing");

        f.write(DATA.as_bytes()).expect("data to be written");
        f.flush();

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let f = fs.open(&test_file).expect("file open");

        let mut buf = vec![0; 64];
        let pos = 2;
        let length = 2;
        let read = f
            .read_with_pos_length(pos, &mut buf, length)
            .expect("read with position ");

        assert_eq!(length, read);

        fs.delete(&test_dir, true).expect("directory to be deleted");
    }

    #[test]
    fn should_sync() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS Name node to be used: [{}]", hdfs_server_url);

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let test_dir = format!("/{}", generate_unique_name());
        fs.mkdir(&test_dir).expect("root dir created");
        assert!(fs.exist(&test_dir));

        let test_file = format!("{}/{}", test_dir, generate_unique_name());

        let mut f = fs.create(&test_file).expect("file open for writing");

        f.write(DATA.as_bytes()).expect("data to be written");
        f.sync().expect("file synced");
        f.flush();

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let f = fs.open(&test_file).expect("file open");

        let mut buf = vec![0; 64];
        let pos = 2;
        let length = 2;
        let read = f
            .read_with_pos_length(pos, &mut buf, length)
            .expect("read with position ");

        assert_eq!(length, read);

        fs.delete(&test_dir, true).expect("directory to be deleted");
    }

    #[test]
    fn should_get_file_status() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS Name node to be used: [{}]", hdfs_server_url);

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let test_dir = format!("/{}", generate_unique_name());
        fs.mkdir(&test_dir).expect("root dir created");
        assert!(fs.exist(&test_dir));

        let test_file = format!("{}/{}", test_dir, generate_unique_name());

        let mut f = fs.create(&test_file).expect("file open for writing");

        f.write(DATA.as_bytes()).expect("data to be written");
        f.sync().expect("file synced");
        f.flush();

        let status = fs
            .get_file_status(&test_file)
            .expect("file status to be retrieved");
        assert_eq!(true, status.is_file());
        let list = fs.list_status(&test_dir).expect("list status");
        assert_eq!(1, list.len());

        fs.delete(&test_dir, true).expect("to be deleted");
    }
}
