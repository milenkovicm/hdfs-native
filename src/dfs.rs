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

//use crate::raw::*;
use crate::{from_raw, to_raw};
use libc::{c_char, c_int, c_short, c_void, time_t};
use libhdfs3_sys::*;
use std::cmp::min;
use std::fmt::{Debug, Formatter};
use std::io::{Error, ErrorKind};
use std::io::{Read, Write};
use std::string::String;
use std::sync::Arc;

/// Options for zero-copy read
// removed pub visibility as it does not look like library supports it
//
/// Includes host names where a particular block of a file is stored.
pub struct BlockHosts {
    ptr: *mut *mut *mut c_char,
}

impl Drop for BlockHosts {
    fn drop(&mut self) {
        unsafe { hdfsFreeHosts(self.ptr) };
    }
}

struct HdfsFileInfoPtr {
    pub ptr: *mut hdfsFileInfo,
    pub len: i32,
}

impl Drop for HdfsFileInfoPtr {
    fn drop(&mut self) {
        unsafe { hdfsFreeFileInfo(self.ptr, self.len) };
    }
}

// TODO: I'm not sure about this part, fingers crossed it will be ok
unsafe impl Send for HdfsFileInfoPtr {}
unsafe impl Sync for HdfsFileInfoPtr {}

impl HdfsFileInfoPtr {
    fn new(ptr: *mut hdfsFileInfo) -> HdfsFileInfoPtr {
        HdfsFileInfoPtr { ptr, len: 1 }
    }

    pub fn new_array(ptr: *mut hdfsFileInfo, len: i32) -> HdfsFileInfoPtr {
        HdfsFileInfoPtr { ptr, len }
    }
}

/// Interface that represents the client side information for a file or directory.
pub struct FileStatus {
    raw: Arc<HdfsFileInfoPtr>,
    idx: u32,
}

impl FileStatus {
    #[inline]
    /// create FileStatus from *const hdfsFileInfo
    fn new(ptr: *mut hdfsFileInfo) -> FileStatus {
        FileStatus {
            raw: Arc::new(HdfsFileInfoPtr::new(ptr)),
            idx: 0,
        }
    }

    /// create FileStatus from *const hdfsFileInfo which points
    /// to dynamically allocated array.
    #[inline]
    fn from_array(raw: Arc<HdfsFileInfoPtr>, idx: u32) -> FileStatus {
        FileStatus { raw, idx }
    }

    #[inline]
    fn ptr(&self) -> *const hdfsFileInfo {
        unsafe { self.raw.ptr.offset(self.idx as isize) }
    }

    /// Get the name of the file
    #[inline]
    pub fn name(&self) -> &str {
        from_raw!((*self.ptr()).mName)
    }

    /// Is this a file?
    #[inline]
    pub fn is_file(&self) -> bool {
        match unsafe { &*self.ptr() }.mKind {
            tObjectKind::kObjectKindFile => true,
            tObjectKind::kObjectKindDirectory => false,
        }
    }

    /// Is this a directory?
    #[inline]
    pub fn is_directory(&self) -> bool {
        match unsafe { &*self.ptr() }.mKind {
            tObjectKind::kObjectKindFile => false,
            tObjectKind::kObjectKindDirectory => true,
        }
    }

    /// Get the owner of the file
    #[inline]
    pub fn owner(&self) -> &str {
        from_raw!((*self.ptr()).mOwner)
    }

    /// Get the group associated with the file
    #[inline]
    pub fn group(&self) -> &str {
        from_raw!((*self.ptr()).mGroup)
    }

    /// Get the permissions associated with the file
    #[inline]
    pub fn permission(&self) -> i16 {
        unsafe { &*self.ptr() }.mPermissions
    }

    /// Get the length of this file, in bytes.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        unsafe { &*self.ptr() }.mSize as usize
    }

    /// Get the block size of the file.
    #[inline]
    pub fn block_size(&self) -> usize {
        unsafe { &*self.ptr() }.mBlockSize as usize
    }

    /// Get the replication factor of a file.
    #[inline]
    pub fn replica_count(&self) -> i16 {
        unsafe { &*self.ptr() }.mReplication
    }

    /// Get the last modification time for the file in seconds
    #[inline]
    pub fn last_modified(&self) -> time_t {
        unsafe { &*self.ptr() }.mLastMod
    }

    /// Get the last access time for the file in seconds
    #[inline]
    pub fn last_accced(&self) -> time_t {
        unsafe { &*self.ptr() }.mLastAccess
    }
}

/// Hdfs Filesystem
///

// we have not opted for clone in this case as it would make deallocation
// much harder to track.
// #[derive(Clone)]
pub struct HdfsFs {
    pub url: String,
    raw: hdfsFS,
}

unsafe impl Send for HdfsFs {}
unsafe impl Sync for HdfsFs {}

impl Drop for HdfsFs {
    fn drop(&mut self) {
        unsafe {
            hdfsDisconnect(self.raw);
        }
    }
}

impl Debug for HdfsFs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hdfs").field("url", &self.url).finish()
    }
}

impl HdfsFs {
    /// create HdfsFs instance. Please use HdfsFsCache rather than using this API directly.
    #[inline]
    pub(crate) fn new(url: String, raw: hdfsFS) -> HdfsFs {
        HdfsFs { url, raw }
    }

    /// Get HDFS namenode url
    #[inline]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get a raw pointer of JNI API's HdfsFs
    #[inline]
    pub fn raw(&self) -> hdfsFS {
        self.raw
    }

    /// Open a file for append
    pub fn append(&self, path: &str) -> Result<HdfsFile<'_>, Error> {
        if !self.exist(path) {
            return Err(ErrorKind::NotFound.into());
        }

        let file = unsafe { hdfsOpenFile(self.raw, to_raw!(path), HDFS_APPEND, 0, 0, 0) };

        if file.is_null() {
            Err(Error::last_os_error())
        } else {
            Ok(HdfsFile {
                fs: self,
                path: path.to_owned(),
                file,
            })
        }
    }

    /// set permission
    pub fn chmod(&self, path: &str, mode: i16) -> bool {
        (unsafe { hdfsChmod(self.raw, to_raw!(path), mode as c_short) }) == 0
    }

    pub fn chown(&self, path: &str, owner: &str, group: &str) -> bool {
        (unsafe { hdfsChown(self.raw, to_raw!(path), to_raw!(owner), to_raw!(group)) }) == 0
    }

    #[inline]
    pub fn create(&self, path: &str) -> Result<HdfsFile<'_>, Error> {
        self.create_with_params(path, false, 0, 0, 0)
    }

    #[inline]
    pub fn create_with_overwrite(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<HdfsFile<'_>, Error> {
        self.create_with_params(path, overwrite, 0, 0, 0)
    }
    // changed visibility to private as buf size has not been respected
    // in the library
    fn create_with_params(
        &self,
        path: &str,
        overwrite: bool,
        buf_size: i32,
        replica_num: i16,
        block_size: i64,
    ) -> Result<HdfsFile<'_>, Error> {
        if !overwrite && self.exist(path) {
            return Err(ErrorKind::AlreadyExists.into());
        }

        let file = unsafe {
            hdfsOpenFile(
                self.raw,
                to_raw!(path),
                HDFS_WRITE,
                buf_size as c_int,
                replica_num as c_short,
                block_size,
            )
        };

        if file.is_null() {
            Err(Error::last_os_error())
        } else {
            Ok(HdfsFile {
                fs: self,
                path: path.to_owned(),
                file,
            })
        }
    }

    /// Get the default blocksize.
    pub fn default_blocksize(&self) -> Result<usize, Error> {
        let block_sz = unsafe { hdfsGetDefaultBlockSize(self.raw) };

        if block_sz > 0 {
            Ok(block_sz as usize)
        } else {
            Err(Error::last_os_error())
        }
    }

    /// Get the default blocksize at the filesystem indicated by a given path.
    // pub fn block_size(&self, path: &str) -> Result<usize, Error> {
    //     let block_sz = unsafe { hdfsGetDefaultBlockSizeAtPath(self.raw, to_raw!(path)) };

    //     if block_sz > 0 {
    //         Ok(block_sz as usize)
    //     } else {
    //         Err(Error::Unknown)
    //     }
    // }

    /// Return the raw capacity of the filesystem.
    pub fn capacity(&self) -> Result<usize, Error> {
        let block_sz = unsafe { hdfsGetCapacity(self.raw) };

        if block_sz < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(block_sz as usize)
        }
    }

    /// Delete file.
    pub fn delete(&self, path: &str, recursive: bool) -> Result<bool, Error> {
        let res = unsafe { hdfsDelete(self.raw, to_raw!(path), recursive as c_int) };

        if res == 0 {
            Ok(true)
        } else {
            Err(Error::last_os_error())
        }
    }

    /// Checks if a given path exsits on the filesystem
    pub fn exist(&self, path: &str) -> bool {
        unsafe { hdfsExists(self.raw, to_raw!(path)) == 0 }
    }

    /// Get hostnames where a particular block (determined by
    /// pos & blocksize) of a file is stored. The last element in the array
    /// is NULL. Due to replication, a single block could be present on
    /// multiple hosts.
    pub fn get_hosts(&self, path: &str, start: usize, length: usize) -> Result<BlockHosts, Error> {
        let ptr = unsafe { hdfsGetHosts(self.raw, to_raw!(path), start as i64, length as i64) };

        if !ptr.is_null() {
            Ok(BlockHosts { ptr })
        } else {
            Err(Error::last_os_error())
        }
    }

    /// create a directory
    pub fn mkdir(&self, path: &str) -> Result<bool, Error> {
        if unsafe { hdfsCreateDirectory(self.raw, to_raw!(path)) } == 0 {
            Ok(true)
        } else {
            Err(Error::last_os_error())
        }
    }

    /// open a file to read
    #[inline]
    pub fn open(&self, path: &str) -> Result<HdfsFile<'_>, Error> {
        self.open_with_bufsize(path, 0)
    }
    // changed visibility to private as buf size has not been respected
    // in the library
    /// open a file to read with a buffer size
    fn open_with_bufsize(&self, path: &str, buf_size: i32) -> Result<HdfsFile<'_>, Error> {
        let file =
            unsafe { hdfsOpenFile(self.raw, to_raw!(path), HDFS_READ, buf_size as c_int, 0, 0) };

        if file.is_null() {
            Err(Error::last_os_error())
        } else {
            Ok(HdfsFile {
                fs: self,
                path: path.to_owned(),
                file,
            })
        }
    }

    /// Set the replication of the specified file to the supplied value
    pub fn set_replication(&self, path: &str, num: i16) -> Result<bool, Error> {
        let res = unsafe { hdfsSetReplication(self.raw, to_raw!(path), num) };

        if res == 0 {
            Ok(true)
        } else {
            Err(Error::last_os_error())
        }
    }

    /// Rename file.
    pub fn rename(&self, old_path: &str, new_path: &str) -> Result<bool, Error> {
        let res = unsafe { hdfsRename(self.raw, to_raw!(old_path), to_raw!(new_path)) };

        if res == 0 {
            Ok(true)
        } else {
            Err(Error::last_os_error())
        }
    }

    /// Return the total raw size of all files in the filesystem.
    pub fn used(&self) -> Result<usize, Error> {
        let block_sz = unsafe { hdfsGetUsed(self.raw) };

        if block_sz < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(block_sz as usize)
        }
    }

    pub fn list_status(&self, path: &str) -> Result<Vec<FileStatus>, Error> {
        let mut entry_num: c_int = 0;

        let ptr = unsafe { hdfsListDirectory(self.raw, to_raw!(path), &mut entry_num) };

        if ptr.is_null() {
            return Err(Error::last_os_error());
        }

        let shared_ptr = Arc::new(HdfsFileInfoPtr::new_array(ptr, entry_num));

        let mut list = Vec::new();
        for idx in 0..entry_num {
            list.push(FileStatus::from_array(shared_ptr.clone(), idx as u32));
        }

        Ok(list)
    }

    pub fn get_file_status(&self, path: &str) -> Result<FileStatus, Error> {
        let ptr = unsafe { hdfsGetPathInfo(self.raw, to_raw!(path)) };

        if ptr.is_null() {
            Err(Error::last_os_error())
        } else {
            Ok(FileStatus::new(ptr))
        }
    }

    pub fn get_last_error() -> &'static str {
        let char_ptr = unsafe { libhdfs3_sys::hdfsGetLastError() };

        if !char_ptr.is_null() {
            from_raw!(char_ptr)
        } else {
            ""
        }
    }
}

/// open hdfs file
pub struct HdfsFile<'a> {
    fs: &'a HdfsFs,
    path: String,
    file: hdfsFile,
}

impl<'a> Drop for HdfsFile<'a> {
    fn drop(&mut self) {
        if self.is_writable() {
            self.flush();
        }
        // this is due to clippy suggestion
        let _ = self.close();
    }
}

impl<'a> Debug for HdfsFile<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsFile")
            .field("fs", &self.fs)
            .field("path", &self.path)
            .field("file", &self.file)
            .finish()
    }
}

impl<'a> HdfsFile<'a> {
    pub fn available(&self) -> Result<bool, Error> {
        if unsafe { hdfsAvailable(self.fs.raw, self.file) } == 0 {
            Ok(true)
        } else {
            Err(Error::last_os_error())
        }
    }

    /// Close the opened file
    fn close(&self) -> Result<bool, Error> {
        if unsafe { hdfsCloseFile(self.fs.raw, self.file) } == 0 {
            Ok(true)
        } else {
            Err(Error::last_os_error())
        }
    }

    /// Flush the data.
    pub fn flush(&mut self) -> bool {
        (unsafe { hdfsFlush(self.fs.raw, self.file) }) == 0
    }

    /// Flush out the data in client's user buffer. After the return of this
    /// call, new readers will see the data.
    pub fn hflush(&mut self) -> bool {
        (unsafe { hdfsHFlush(self.fs.raw, self.file) }) == 0
    }

    /// Similar to posix fsync, Flush out the data in client's
    /// user buffer. all the way to the disk device (but the disk may have
    /// it in its cache).
    // pub fn hsync(&self) -> bool {
    //     (unsafe { hdfsHSync(self.fs.raw, self.file) }) == 0
    // }

    /// Determine if a file is open for read.
    pub fn is_readable(&self) -> bool {
        (unsafe { hdfsFileIsOpenForRead(self.file) }) == 1
    }

    /// Determine if a file is open for write.
    pub fn is_writable(&self) -> bool {
        (unsafe { hdfsFileIsOpenForWrite(self.file) }) == 1
    }

    /// Return a file path
    pub fn path(&'a self) -> &'a str {
        &self.path
    }

    /// Get the current offset in the file, in bytes.
    pub fn pos(&self) -> Result<u64, Error> {
        let pos = unsafe { hdfsTell(self.fs.raw, self.file) };

        if pos < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(pos as u64)
        }
    }

    /// Read data from an open file.
    pub fn read(&self, buf: &mut [u8]) -> Result<usize, Error> {
        let read_len = unsafe {
            hdfsRead(
                self.fs.raw,
                self.file,
                buf.as_ptr() as *mut c_void,
                buf.len() as tSize,
            )
        };

        if read_len < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(read_len as usize)
        }
    }

    /// Positional read of data from an open file.
    pub fn read_with_pos(&self, pos: i64, buf: &mut [u8]) -> Result<usize, Error> {
        // let read_len = unsafe {
        //     hdfsPread(
        //         self.fs.raw,
        //         self.file,
        //         pos as tOffset,
        //         buf.as_ptr() as *mut c_void,
        //         buf.len() as tSize,
        //     )
        // };
        // let seek_result = self.seek(pos as u64);

        if self.seek(pos as u64) {
            self.read(buf)
        } else {
            Err(Error::last_os_error())
        }
    }

    /// Read data from an open file.
    pub fn read_length(&self, buf: &mut [u8], length: usize) -> Result<usize, Error> {
        let required_len = min(length, buf.len());
        let read_len = unsafe {
            hdfsRead(
                self.fs.raw,
                self.file,
                buf.as_ptr() as *mut c_void,
                required_len as tSize,
            )
        };

        if read_len < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(read_len as usize)
        }
    }

    /// Positional read of data from an open file.
    pub fn read_with_pos_length(
        &self,
        pos: i64,
        buf: &mut [u8],
        length: usize,
    ) -> Result<usize, Error> {
        let required_len = min(length, buf.len());

        if !self.seek(pos as u64) {
            return Err(Error::last_os_error());
        }

        let read_len = unsafe {
            hdfsRead(
                self.fs.raw,
                self.file,
                buf.as_ptr() as *mut c_void,
                required_len as tSize,
            )
        };

        if read_len < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(read_len as usize)
        }
    }

    /// Seek to given offset in file.
    pub fn seek(&self, offset: u64) -> bool {
        (unsafe { hdfsSeek(self.fs.raw, self.file, offset as tOffset) }) == 0
    }

    /// Write data into an open file.
    pub fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let written_len = unsafe {
            hdfsWrite(
                self.fs.raw,
                self.file,
                buf.as_ptr() as *mut c_void,
                buf.len() as tSize,
            )
        };

        if written_len < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(written_len as usize)
        }
    }

    pub fn sync(&mut self) -> Result<(), Error> {
        let written_len = unsafe { hdfsSync(self.fs.raw, self.file) };

        if written_len < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }
    // consider adding it
    //
    // pub fn get_file_status(&self) -> Result<FileStatus, Error> {
    //     if self. ptr().is_null() {
    //         Err(Error::Unknown)
    //     } else {
    //         Ok(FileStatus::new(self.ptr().clone()))
    //     }
    // }
}

impl<'a> Write for HdfsFile<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        HdfsFile::write(self, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        HdfsFile::flush(self);
        Ok(())
    }
}

impl<'a> Read for HdfsFile<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        HdfsFile::read(self, buf)
    }
}
