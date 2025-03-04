mod common;

#[cfg(test)]
mod e2e {

    use std::{io::Write, sync::Arc};

    use crate::common::*;
    use hdfs_native::HdfsRegistry;
    use log::info;

    #[test]
    fn should_write_a_lot_of_data() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS Name node to be used: [{}]", hdfs_server_url);

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let test_file = format!("/{}", generate_unique_name());

        let mut file = fs.create(&test_file).expect("file to be created");
        let block_write = 1024 * 1024;
        let total_writes = 100;
        let data = vec![50; block_write];

        for _ in 0..total_writes {
            file.write_all(&data).expect("data written");
        }

        drop(file);
        let file = fs.get_file_status(&test_file).expect("file status");

        assert_eq!(total_writes * block_write, file.len());

        fs.delete(&test_file, false).expect("file to be deleted");
    }

    #[test]
    fn should_write_a_lot_of_data_in_threads() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS Name node to be used: [{}]", hdfs_server_url);

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let total_threads = 4;
        let barrier = Arc::new(std::sync::Barrier::new(total_threads));
        let mut handles = vec![];
        for _ in 0..total_threads {
            let barrier = barrier.clone();
            let fs = fs.clone();
            let t = std::thread::spawn(move || {
                info!("waiting for threads to startup ...");
                barrier.wait();
                let test_file = format!("/{}", generate_unique_name());
                info!("creating test file: {} and writing ...", test_file);
                let mut file = fs.create(&test_file).expect("file to be created");
                let block_write = 1024 * 1024;
                let total_writes = 10;
                let data = vec![77; block_write];

                for _ in 0..total_writes {
                    file.write_all(&data).expect("data written");
                }

                drop(file);
                let file = fs.get_file_status(&test_file).expect("file status");

                assert_eq!(total_writes * block_write, file.len());

                fs.delete(&test_file, false).expect("file to be deleted");
                info!("finishing with test file: {}", test_file);
            });
            handles.push(t)
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
