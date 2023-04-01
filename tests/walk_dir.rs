mod common;

#[cfg(test)]
mod e2e {
    use std::sync::Arc;

    use crate::common::*;
    use hdfs_native::{walk_dir::HdfsWalkDir, HdfsErr, HdfsFs, HdfsRegistry};
    use log::info;

    #[test]
    fn walk_dir_sanity() {
        let fs_registry = HdfsRegistry::new();
        let hdfs_server_url = generate_hdfs_url();
        info!("walk_dir test ...");

        let fs = fs_registry
            .get(&hdfs_server_url)
            .expect("creation of registry");

        let hdfs = Arc::new(fs);

        match set_up_hdfs_env(hdfs.clone()) {
            _ => (),
        };

        let hdfs_walk_dir = HdfsWalkDir::new_with_hdfs("/testing".to_owned(), hdfs.clone())
            .min_depth(0)
            .max_depth(2);

        let mut iter = hdfs_walk_dir.into_iter();

        let ret_vec = [
            "/testing",
            "/testing/c",
            "/testing/b",
            "/testing/b/3",
            "/testing/b/2",
            "/testing/b/1",
            "/testing/a",
            "/testing/a/3",
            "/testing/a/2",
            "/testing/a/1",
        ];
        for entry in ret_vec.into_iter() {
            assert_eq!(
                entry,
                // format!("{}{}", hdfs.url(), entry),
                iter.next().unwrap().unwrap().name()
            );
        }
        assert!(iter.next().is_none());

        let hdfs_walk_dir = HdfsWalkDir::new_with_hdfs("/testing".to_owned(), hdfs.clone())
            .min_depth(2)
            .max_depth(3);
        let mut iter = hdfs_walk_dir.into_iter();

        let ret_vec = [
            "/testing/b/3",
            "/testing/b/2",
            "/testing/b/1",
            "/testing/a/3",
            "/testing/a/2",
            "/testing/a/2/11",
            "/testing/a/1",
            "/testing/a/1/12",
            "/testing/a/1/11",
        ];
        for entry in ret_vec.into_iter() {
            assert_eq!(
                entry,
                // format!("{}{}", hdfs.url(), entry),
                iter.next().unwrap().unwrap().name()
            );
        }
        assert!(iter.next().is_none());

        hdfs.delete("/testing/", true)
            .expect("directory to be deleted");
    }

    fn set_up_hdfs_env(hdfs: Arc<HdfsFs>) -> Result<Arc<HdfsFs>, HdfsErr> {
        hdfs.mkdir("/testing")?;
        hdfs.mkdir("/testing/a")?;
        hdfs.mkdir("/testing/b")?;
        hdfs.create("/testing/c")?;
        hdfs.mkdir("/testing/a/1")?;
        hdfs.mkdir("/testing/a/2")?;
        hdfs.create("/testing/a/3")?;
        hdfs.create("/testing/b/1")?;
        hdfs.create("/testing/b/2")?;
        hdfs.create("/testing/b/3")?;
        hdfs.create("/testing/a/1/11")?;
        hdfs.create("/testing/a/1/12")?;
        hdfs.create("/testing/a/2/11")?;

        Ok(hdfs)
    }
}
