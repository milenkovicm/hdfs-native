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

use std::io::ErrorKind;
use thiserror::Error;

use crate::from_raw;

/// Errors which can occur during accessing Hdfs cluster
#[derive(Error, Debug)]
pub enum HdfsErr {
    #[error("Unknown HDFS error")]
    Unknown,
    /// wrapper around IO error
    #[error("{0}")]
    IoError(#[from] std::io::Error),
    /// file path
    #[error("File not found `{0}`")]
    FileNotFound(String),
    /// file path           
    #[error("File already exists `{0}`")]
    FileAlreadyExists(String),
    /// namenode address
    #[error("Cannot connect to NameNode `{0}`")]
    CannotConnectToNameNode(String),
    /// URL
    #[error("Invalid URL `{0}`")]
    InvalidUrl(String),
}

impl HdfsErr {
    /// create error based on `errno` set by the library
    pub fn from_errno() -> HdfsErr {
        let e = errno::errno();
        HdfsErr::IoError(std::io::Error::from_raw_os_error(e.0))
    }
}

fn get_error_kind(e: &HdfsErr) -> ErrorKind {
    match e {
        HdfsErr::Unknown => ErrorKind::Other,
        HdfsErr::FileNotFound(_) => ErrorKind::NotFound,
        HdfsErr::FileAlreadyExists(_) => ErrorKind::AlreadyExists,
        HdfsErr::CannotConnectToNameNode(_) => ErrorKind::ConnectionRefused,
        HdfsErr::InvalidUrl(_) => ErrorKind::AddrNotAvailable,
        HdfsErr::IoError(_) => ErrorKind::Other, // we don't care about this one as will explicitly match it in from
    }
}

impl From<HdfsErr> for std::io::Error {
    fn from(e: HdfsErr) -> std::io::Error {
        match e {
            HdfsErr::IoError(e) => e,
            e => {
                let transformed_kind = get_error_kind(&e);
                std::io::Error::new(transformed_kind, e)
            }
        }
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
