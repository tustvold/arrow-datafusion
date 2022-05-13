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

use futures::FutureExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

pub fn make_in_memory_store<I, K>(files: I) -> Arc<dyn ObjectStore>
where
    I: IntoIterator<Item = (K, usize)>,
    K: AsRef<str>,
{
    let store = InMemory::new();
    for (k, len) in files {
        store
            .put(&Path::from_raw(k), vec![0; len].into())
            .now_or_never()
            .unwrap()
            .unwrap();
    }
    Arc::new(store)
}

