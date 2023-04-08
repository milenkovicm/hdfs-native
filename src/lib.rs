// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A rust wrapper over libhdfs3

/// Rust APIs wrapping libhdfs3 API, providing better semantic and abstraction
pub mod dfs;
/// libhdfs3 raw binding APIs
pub mod raw;
pub mod util;

pub use crate::dfs::*;
pub use crate::util::HdfsUtil;

use crate::raw::{
    hdfsBuilderConnect, hdfsBuilderSetNameNode, hdfsBuilderSetNameNodePort, hdfsFS, hdfsNewBuilder,
};
use log::{debug, info};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use url::Url;

static LOCAL_FS_SCHEME: &str = "file";

/// HdfsRegistry which stores seen HdfsFs instances.
#[derive(Debug)]
pub struct HdfsRegistry {
    all_fs: Arc<Mutex<HashMap<String, Arc<HdfsFs>>>>,
}

impl Default for HdfsRegistry {
    fn default() -> Self {
        HdfsRegistry::new()
    }
}

struct HostPort {
    host: String,
    port: u16,
}

enum NNScheme {
    Local,
    Remote(HostPort),
}

impl ToString for NNScheme {
    fn to_string(&self) -> String {
        match self {
            NNScheme::Local => "file:///".to_string(),
            NNScheme::Remote(hp) => format!("{}:{}", hp.host, hp.port),
        }
    }
}

impl HdfsRegistry {
    pub fn new() -> HdfsRegistry {
        HdfsRegistry {
            all_fs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_name_node(&self, path: &str) -> Result<NNScheme, Error> {
        match Url::parse(path) {
            Ok(url) => {
                if url.scheme() == LOCAL_FS_SCHEME {
                    Ok(NNScheme::Local)
                } else if url.host().is_some() && url.port().is_some() {
                    Ok(NNScheme::Remote(HostPort {
                        host: format!("{}://{}", &url.scheme(), url.host().unwrap()),
                        port: url.port().unwrap(),
                    }))
                } else {
                    Err(ErrorKind::InvalidInput.into())
                }
            }
            Err(_) => Err(ErrorKind::AddrNotAvailable.into()),
        }
    }

    pub fn get(&self, path: &str) -> Result<Arc<HdfsFs>, Error> {
        let host_port = self.get_name_node(path)?;

        let mut map = self.all_fs.lock().unwrap();

        let entry: &mut Arc<HdfsFs> = map.entry(host_port.to_string()).or_insert({
            let hdfs_fs: *const hdfsFS = unsafe {
                let hdfs_builder = hdfsNewBuilder();
                match host_port {
                    NNScheme::Local => {}
                    NNScheme::Remote(ref hp) => {
                        hdfsBuilderSetNameNode(hdfs_builder, to_raw!(&*hp.host));
                        hdfsBuilderSetNameNodePort(hdfs_builder, hp.port);
                    }
                }
                info!(
                    "Connecting to NameNode ... url: [{}]",
                    &host_port.to_string()
                );
                hdfsBuilderConnect(hdfs_builder)
            };

            if hdfs_fs.is_null() {
                return Err(ErrorKind::AddrNotAvailable.into());
            }
            debug!("Connected to NameNode, url: [{}]", &host_port.to_string());

            Arc::new(HdfsFs::new(host_port.to_string(), hdfs_fs))
        });

        Ok(entry.clone())
    }
}

pub fn get_last_error() -> &'static str {
    let char_ptr = unsafe { crate::raw::hdfsGetLastError() };

    if !char_ptr.is_null() {
        from_raw!(char_ptr)
    } else {
        ""
    }
}
