mod common;

#[cfg(test)]
mod e2e {

    use std::io::ErrorKind;

    use crate::common::*;
    use hdfs_native::{err::*, HdfsRegistry};

    const DATA: &str = "1234567890";

    #[test]
    fn should_return_error_message() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let error = get_last_error();
        assert_eq!("Success", error);

        let test_file = format!("/{}", generate_unique_name());
        let mut w = fs.create(&test_file).expect("file to be created");
        w.write(DATA.as_bytes()).expect("data to be written");

        let w = fs.append(&test_file);
        match w {
            _ => (),
        }

        let error = get_last_error();
        println!("{:?}", error);
        let expected_message = "Failed to APPEND_FILE";

        assert_eq!(expected_message, &error[0..expected_message.len()]);

        fs.delete(&test_file, false).expect("file to be deleted");
    }

    #[test]
    fn should_return_errno() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let error = get_last_error();
        assert_eq!("Success", error);

        let file_does_not_exist = format!("/{}", generate_unique_name());
        let result = fs.open(&file_does_not_exist);
        if let Err(hdfs_native::err::HdfsErr::IoError(e)) = result {
            assert_eq!(ErrorKind::NotFound, e.kind())
        } else {
            assert!(false)
        }
    }
}
