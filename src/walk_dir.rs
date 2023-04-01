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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::err::HdfsErr;
use crate::{FileStatus, HdfsFs};
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
pub struct HdfsWalkDir {
    hdfs: Arc<HdfsFs>,
    root: String,
    opts: IterOptions,
}

impl HdfsWalkDir {
    // pub fn new(root: String, hdfs_registry: &HdfsRegistry) -> Result<Self, HdfsErr> {
    //     let hdfs = hdfs_registry.get(&root)?;

    //     Ok(Self::new_with_hdfs(root, Arc::new(hdfs)))
    // }

    pub fn new_with_hdfs(root: String, hdfs: Arc<HdfsFs>) -> Self {
        Self {
            hdfs,
            root,
            opts: IterOptions {
                min_depth: 0,
                max_depth: ::std::usize::MAX,
            },
        }
    }

    /// Set the minimum depth of entries yielded by the iterator.
    ///
    /// The smallest depth is `0` and always corresponds to the path given
    /// to the `new` function on this type. Its direct descendents have depth
    /// `1`, and their descendents have depth `2`, and so on.
    pub fn min_depth(mut self, depth: usize) -> Self {
        self.opts.min_depth = depth;
        if self.opts.min_depth > self.opts.max_depth {
            self.opts.min_depth = self.opts.max_depth;
        }
        self
    }

    /// Set the maximum depth of entries yield by the iterator.
    ///
    /// The smallest depth is `0` and always corresponds to the path given
    /// to the `new` function on this type. Its direct descendents have depth
    /// `1`, and their descendents have depth `2`, and so on.
    ///
    /// Note that this will not simply filter the entries of the iterator, but
    /// it will actually avoid descending into directories when the depth is
    /// exceeded.
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.opts.max_depth = depth;
        if self.opts.max_depth < self.opts.min_depth {
            self.opts.max_depth = self.opts.min_depth;
        }
        self
    }
}

impl IntoIterator for HdfsWalkDir {
    type Item = Result<FileStatus, HdfsErr>;
    type IntoIter = TreeIter<String, FileStatus, HdfsErr>;

    fn into_iter(self) -> TreeIter<String, FileStatus, HdfsErr> {
        TreeIter::new(
            Box::new(HdfsTreeManager {
                hdfs: self.hdfs.clone(),
            }),
            self.opts,
            self.root,
        )
    }
}

struct HdfsTreeManager {
    hdfs: Arc<HdfsFs>,
}

impl TreeManager<String, FileStatus, HdfsErr> for HdfsTreeManager {
    fn to_value(&self, v: String) -> Result<FileStatus, HdfsErr> {
        self.hdfs.get_file_status(&v)
    }

    fn get_children(&self, n: &FileStatus) -> Result<Vec<FileStatus>, HdfsErr> {
        self.hdfs.list_status(n.name())
    }

    fn is_leaf(&self, n: &FileStatus) -> bool {
        n.is_file()
    }
}

/// A utility struct for iterator tree nodes
pub struct TreeIter<V, N, E> {
    /// For checking node properties, like leaf node or not, finding children, etc.
    manager: Box<dyn TreeManager<V, N, E>>,
    /// Options specified in the builder. Depths, etc.
    opts: IterOptions,
    /// The start path.
    /// This is only `Some(...)` at the beginning. After the first iteration,
    /// this is always `None`.
    start: Option<V>,
    /// A stack of unvisited qualified entries
    stack_nodes: Vec<TreeNode<N>>,
    /// A stack of unqualified parent entries
    deferred_nodes: Vec<TreeNode<N>>,
}

impl<V, N, E> TreeIter<V, N, E> {
    pub fn new(
        manager: Box<dyn TreeManager<V, N, E>>,
        opts: IterOptions,
        start: V,
    ) -> TreeIter<V, N, E> {
        TreeIter {
            manager,
            opts,
            start: Some(start),
            stack_nodes: vec![],
            deferred_nodes: vec![],
        }
    }

    fn next_item(&mut self) -> Result<Option<N>, E> {
        // Initialize if possible
        if let Some(start) = self.start.take() {
            let root = self.manager.to_value(start)?;
            match (0 == self.opts.min_depth, self.manager.is_leaf(&root)) {
                (true, true) => return Ok(Some(root)),
                (true, false) => self.stack_nodes.push(TreeNode {
                    node: root,
                    layer: 0,
                }),
                (false, true) => return Ok(None),
                (false, false) => self.deferred_nodes.push(TreeNode {
                    node: root,
                    layer: 0,
                }),
            }
        }

        // Check whether there are items in the qualified layer
        if let Some(next_node) = self.stack_nodes.pop() {
            if next_node.layer < self.opts.max_depth && !self.manager.is_leaf(&next_node.node) {
                self.stack_nodes.extend(
                    self.manager
                        .get_children(&next_node.node)?
                        .into_iter()
                        .map(|node| TreeNode {
                            node,
                            layer: next_node.layer + 1,
                        }),
                );
            }
            return Ok(Some(next_node.node));
        }

        // Check deferred nodes whose children is not empty
        if let Some(prev_node) = self.deferred_nodes.pop() {
            assert!(!self.manager.is_leaf(&prev_node.node));
            let children = self.manager.get_children(&prev_node.node)?;
            if prev_node.layer + 1 == self.opts.min_depth {
                self.stack_nodes
                    .extend(children.into_iter().map(|node| TreeNode {
                        node,
                        layer: prev_node.layer + 1,
                    }));
            } else {
                let result: Vec<TreeNode<N>> = children
                    .into_iter()
                    .filter(|node| !self.manager.is_leaf(node))
                    .map(|node| TreeNode {
                        node,
                        layer: prev_node.layer + 1,
                    })
                    .collect();

                self.deferred_nodes.extend(result);
            }
            return self.next_item();
        }

        Ok(None)
    }
}

impl<V, N, E> Iterator for TreeIter<V, N, E> {
    type Item = Result<N, E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item().transpose()
    }
}

pub struct TreeNode<N> {
    node: N,
    layer: usize,
}

pub struct IterOptions {
    pub min_depth: usize,
    pub max_depth: usize,
}

impl fmt::Debug for IterOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterOptions")
            .field("min_depth", &self.min_depth)
            .field("max_depth", &self.max_depth)
            .finish()
    }
}

pub trait TreeManager<V, N, E>: Send + Sync {
    fn to_value(&self, v: V) -> Result<N, E>;

    fn get_children(&self, n: &N) -> Result<Vec<N>, E>;

    fn is_leaf(&self, n: &N) -> bool;
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::io::Error;

    use super::{IterOptions, TreeIter, TreeManager};

    #[test]
    fn test_tree_iter() -> Result<(), Error> {
        let mut iter = TreeIter::new(
            Box::new(create_test_tree_manager()),
            IterOptions {
                min_depth: 0,
                max_depth: 2,
            },
            "/testing".to_owned(),
        );

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
        for entry in ret_vec.iter() {
            assert_eq!(entry.to_owned(), iter.next().unwrap()?);
        }
        assert!(iter.next().is_none());

        let mut iter = TreeIter::new(
            Box::new(create_test_tree_manager()),
            IterOptions {
                min_depth: 2,
                max_depth: 3,
            },
            "/testing".to_owned(),
        );

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
        for entry in ret_vec.iter() {
            assert_eq!(entry.to_owned(), iter.next().unwrap()?);
        }
        assert!(iter.next().is_none());
        Ok(())
    }

    // Testing data with pairs. The first one indicating the node value. The second one indicating whether it's a leaf node or not.
    fn create_test_tree_manager() -> TestTreeManager {
        TestTreeManager {
            data: BTreeMap::from([
                ("/testing".to_owned(), false),
                ("/testing/a".to_owned(), false),
                ("/testing/b".to_owned(), false),
                ("/testing/c".to_owned(), true),
                ("/testing/a/1".to_owned(), false),
                ("/testing/a/2".to_owned(), false),
                ("/testing/a/3".to_owned(), true),
                ("/testing/b/1".to_owned(), true),
                ("/testing/b/2".to_owned(), true),
                ("/testing/b/3".to_owned(), true),
                ("/testing/a/1/11".to_owned(), true),
                ("/testing/a/1/12".to_owned(), true),
                ("/testing/a/2/11".to_owned(), true),
            ]),
        }
    }

    struct TestTreeManager {
        data: BTreeMap<String, bool>,
    }

    impl TreeManager<String, String, Error> for TestTreeManager {
        fn to_value(&self, v: String) -> Result<String, Error> {
            Ok(v)
        }

        fn get_children(&self, n: &String) -> Result<Vec<String>, Error> {
            Ok(self
                .data
                .keys()
                .filter(|entry| {
                    entry.len() > n.len()
                        && entry.starts_with(n)
                        && !entry[n.len() + 1..].contains('/')
                })
                .map(|entry| entry.to_owned())
                .collect())
        }

        fn is_leaf(&self, n: &String) -> bool {
            *self.data.get(n).unwrap()
        }
    }
}
