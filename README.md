<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# HDFS-Native

A rust wrapper over libhdfs3 developed by Apache HAWQ.

This repo is fork of [datafusion-contrib/hdfs-native](https://github.com/datafusion-contrib/hdfs-native).

## Setup

1. Install libhdfs3

You can either install it via [Conda](https://docs.conda.io/en/latest/)

```shell
conda install -c conda-forge libhdfs3
```

Or build it from source, but some patching might be required [as shown](https://github.com/conda-forge/libhdfs3-feedstock/tree/main/recipe)

```shell
git clone https://github.com/apache/hawq
cd hawq/depends/libhdfs3

# then build it
mkdir build && cd build
../bootstrap --prefix=/usr/local
make
make install
```

## Configuration

```shell
# client conf to use, env LIBHDFS3_CONF or hdfs-client.xml in working directory
export LIBHDFS3_CONF=/path/to/libhdfs3-hdfs-client.xml
```
