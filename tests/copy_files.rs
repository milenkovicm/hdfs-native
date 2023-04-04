mod common;

#[cfg(test)]

mod copy {

    use crate::common::*;
    use hdfs_native::{util::HdfsUtil, HdfsRegistry};
    use log::info;

    #[test]
    #[ignore = "libhdfs3 does not support this operation"]
    fn should_copy() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS [name node] to be used: [{}]", hdfs_server_url);
        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        assert!(fs.exist("/sbs_original.csv"));
        assert!(!fs.exist("/sbs_copied.csv"));

        let copied = HdfsUtil::cp(&fs, "/sbs_original.csv", &fs, "/sbs_copied.csv")
            .expect("file to be copied");

        assert!(copied)
    }

    #[test]
    #[ignore = "libhdfs3 does not support this operation"]
    fn should_move() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();

        info!("HDFS [name node] to be used: [{}]", hdfs_server_url);
        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        assert!(fs.exist("/sbs_original.csv"));
        assert!(!fs.exist("/sbs_moved.csv"));

        let moved = HdfsUtil::mv(&fs, "/sbs_original.csv", &fs, "/sbs_moved.csv")
            .expect("file to be moved");
        assert!(moved)
    }
}
