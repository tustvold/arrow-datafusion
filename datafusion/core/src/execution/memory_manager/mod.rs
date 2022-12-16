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

//! Manages all available memory during query execution

use crate::error::Result;
use std::sync::Arc;

mod pool;
pub mod proxy;

pub use pool::*;

/// The pool of memory from which [`TrackedAllocation`] allocate
pub trait MemoryPool: Send + Sync + std::fmt::Debug {
    /// Records the creation of a new [`TrackedAllocation`] with [`AllocationOptions`]
    fn allocate(&self, _options: &AllocationOptions) {}

    /// Records the destruction of a [`TrackedAllocation`] with [`AllocationOptions`]
    fn free(&self, _options: &AllocationOptions) {}

    /// Infallibly grow the provided `allocation` by `additional` bytes
    ///
    /// This must always succeed
    fn grow(&self, allocation: &TrackedAllocation, additional: usize);

    /// Infallibly shrink the provided `allocation` by `shrink` bytes
    fn shrink(&self, allocation: &TrackedAllocation, shrink: usize);

    /// Attempt to grow the provided `allocation` by `additional` bytes
    ///
    /// On error the `allocation` will not be increased in size
    fn try_grow(&self, allocation: &TrackedAllocation, additional: usize) -> Result<()>;

    /// Return the total amount of memory allocated
    fn allocated(&self) -> usize;
}

/// Options associated with a [`TrackedAllocation`]
#[derive(Debug)]
pub struct AllocationOptions {
    name: String,
    can_spill: bool,
}

impl AllocationOptions {
    /// Create a new [`AllocationOptions`]
    pub fn new(name: String) -> Self {
        Self {
            name,
            can_spill: false,
        }
    }

    /// Set whether this allocation can be spilled to disk
    pub fn with_can_spill(self, can_spill: bool) -> Self {
        Self { can_spill, ..self }
    }

    /// Returns true if this allocation can spill to disk
    pub fn can_spill(&self) -> bool {
        self.can_spill
    }

    /// Returns the name associated with this allocation
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A [`TrackedAllocation`] tracks a reservation of memory in a [`MemoryPool`]
/// that is freed back to the pool on drop
#[derive(Debug)]
pub struct TrackedAllocation {
    options: AllocationOptions,
    size: usize,
    policy: Arc<dyn MemoryPool>,
}

impl TrackedAllocation {
    /// Create a new [`TrackedAllocation`] in the provided [`MemoryPool`]
    pub fn new(pool: &Arc<dyn MemoryPool>, name: String) -> Self {
        Self::new_with_options(pool, AllocationOptions::new(name))
    }

    /// Create a new [`TrackedAllocation`] in the provided [`MemoryPool`]
    pub fn new_with_options(
        pool: &Arc<dyn MemoryPool>,
        options: AllocationOptions,
    ) -> Self {
        pool.allocate(&options);
        Self {
            options,
            size: 0,
            policy: Arc::clone(pool),
        }
    }

    /// Returns the size of this [`TrackedAllocation`] in bytes
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns this allocations [`AllocationOptions`]
    pub fn options(&self) -> &AllocationOptions {
        &self.options
    }

    /// Frees all bytes from this allocation returning the number of bytes freed
    pub fn free(&mut self) -> usize {
        let size = self.size;
        if size != 0 {
            self.shrink(size)
        }
        size
    }

    /// Frees `capacity` bytes from this allocation
    ///
    /// # Panics
    ///
    /// Panics if `capacity` exceeds [`Self::size`]
    pub fn shrink(&mut self, capacity: usize) {
        let new_size = self.size.checked_sub(capacity).unwrap();
        self.policy.shrink(self, capacity);
        self.size = new_size
    }

    /// Sets the size of this allocation to `capacity`
    pub fn resize(&mut self, capacity: usize) {
        use std::cmp::Ordering;
        match capacity.cmp(&self.size) {
            Ordering::Greater => self.grow(capacity - self.size),
            Ordering::Less => self.shrink(self.size - capacity),
            _ => {}
        }
    }

    /// Increase the size of this by `capacity` bytes
    pub fn grow(&mut self, capacity: usize) {
        self.policy.grow(self, capacity);
        self.size += capacity;
    }

    /// Try to increase the size of this [`TrackedAllocation`] by `capacity` bytes
    pub fn try_grow(&mut self, capacity: usize) -> Result<()> {
        self.policy.try_grow(self, capacity)?;
        self.size += capacity;
        Ok(())
    }
}

impl Drop for TrackedAllocation {
    fn drop(&mut self) {
        self.free();
        self.policy.free(&self.options);
    }
}

const TB: u64 = 1 << 40;
const GB: u64 = 1 << 30;
const MB: u64 = 1 << 20;
const KB: u64 = 1 << 10;

/// Present size in human readable form
pub fn human_readable_size(size: usize) -> String {
    let size = size as u64;
    let (value, unit) = {
        if size >= 2 * TB {
            (size as f64 / TB as f64, "TB")
        } else if size >= 2 * GB {
            (size as f64 / GB as f64, "GB")
        } else if size >= 2 * MB {
            (size as f64 / MB as f64, "MB")
        } else if size >= 2 * KB {
            (size as f64 / KB as f64, "KB")
        } else {
            (size as f64, "B")
        }
    };
    format!("{:.1} {}", value, unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_manager_underflow() {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let mut a1 = TrackedAllocation::new(&pool, "a1".to_string());
        assert_eq!(pool.allocated(), 0);

        a1.grow(100);
        assert_eq!(pool.allocated(), 100);

        assert_eq!(a1.free(), 100);
        assert_eq!(pool.allocated(), 0);

        a1.try_grow(100).unwrap_err();
        assert_eq!(pool.allocated(), 0);

        a1.try_grow(30).unwrap();
        assert_eq!(pool.allocated(), 30);

        let mut a2 = TrackedAllocation::new(&pool, "a2".to_string());
        a2.try_grow(25).unwrap_err();
        assert_eq!(pool.allocated(), 30);

        drop(a1);
        assert_eq!(pool.allocated(), 0);

        a2.try_grow(25).unwrap();
        assert_eq!(pool.allocated(), 25);
    }
}
