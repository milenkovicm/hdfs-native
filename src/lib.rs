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
pub mod util;
pub use crate::dfs::*;
pub use crate::util::HdfsUtil;
use libhdfs3_sys::*;
use log::{debug, info};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use url::Url;

pub mod raw {
    pub use libhdfs3_sys::*;
}

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
            Err(_) => Err(ErrorKind::InvalidInput.into()),
        }
    }

    pub fn get(&self, path: &str) -> Result<Arc<HdfsFs>, Error> {
        debug!("fs get for path: [{}]", path);
        let host_port = self.get_name_node(path)?;
        let mut map = self.all_fs.lock().unwrap();

        let entry: &mut Arc<HdfsFs> = map.entry(host_port.to_string()).or_insert({
            debug!("fs get for path: [{}] ... creating new FS", path);
            let mut builder = HdfsBuilder::builder();
            match host_port {
                NNScheme::Local => return Err(ErrorKind::Unsupported.into()),
                NNScheme::Remote(ref hp) => {
                    builder.set_name_node(&hp.host);
                    builder.set_name_port(hp.port);
                }
            }
            let fs = builder.connect()?;
            info!("fs get for path: [{}] ... connected", path);
            Arc::new(fs)
        });

        Ok(entry.clone())
    }
}

pub struct HdfsBuilder {
    builder: *mut hdfsBuilder,
    host: String,
    port: u16,
}

impl HdfsBuilder {
    pub fn builder() -> Self {
        let builder: *mut hdfsBuilder = unsafe { hdfsNewBuilder() };
        let host = "default".to_string();
        let port = 0;
        Self { builder, host, port }
    }

    pub fn connect_name_node(host: &str, port: u16 ) -> Result<HdfsFs, Error> {
        let hdfs_fs = unsafe {
            hdfsConnectNewInstance(to_raw!(host), port)
        };

        if hdfs_fs.is_null() {
            Err(Error::last_os_error())
        } else {
            let host_port = format!("{}:{}", host, port);
            Ok(HdfsFs::new(host_port, hdfs_fs))
        }
    }

    pub fn connect_name_node_as_user(host: &str, port: u16, user: &str ) -> Result<HdfsFs, Error> {
        let hdfs_fs = unsafe {
            hdfsConnectAsUserNewInstance(to_raw!(host), port, to_raw!(user))
        };

        if hdfs_fs.is_null() {
            Err(Error::last_os_error())
        } else {
            let host_port = format!("{}:{}", host, port);
            Ok(HdfsFs::new(host_port, hdfs_fs))
        }
    }

    pub fn set_name_node_port(&mut self, host: &str, port: u16) {
        self.host = host.to_string();
        self.port = port;
        unsafe {
            hdfsBuilderSetNameNode(self.builder, to_raw!(host));
            hdfsBuilderSetNameNodePort(self.builder, port);
        }
    }

    pub fn set_name_node(&mut self, host: &str) {
        self.host = host.to_string();

        unsafe {
            hdfsBuilderSetNameNode(self.builder, to_raw!(host));
        }
    }

    pub fn set_name_port(&mut self, port: u16) {
        self.port = port;
        unsafe {
            hdfsBuilderSetNameNodePort(self.builder, port);
        }
    }
    pub fn connect(self) -> Result<HdfsFs, Error> {
        let hdfs_fs = unsafe { hdfsBuilderConnect(self.builder) };

        if hdfs_fs.is_null() {
            Err(Error::last_os_error())
        } else {
            let host_port = format!("{}:{}", self.host, self.port);
            Ok(HdfsFs::new(host_port, hdfs_fs))
        }
    }
}

impl Drop for HdfsBuilder {
    fn drop(&mut self) {
        unsafe { hdfsFreeBuilder(self.builder) }
    }
}

impl Default for HdfsBuilder {
    fn default() -> Self {
        Self::builder()
    }
}
