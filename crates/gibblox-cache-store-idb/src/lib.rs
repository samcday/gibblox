#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

use alloc::{boxed::Box, string::String};
use async_trait::async_trait;
use gibblox_cache::CacheStore;
use gibblox_core::{GibbloxError, GibbloxErrorKind, GibbloxResult};

#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(target_arch = "wasm32")]
pub use wasm::IdbCacheStore;

#[cfg(not(target_arch = "wasm32"))]
pub struct IdbCacheStore;

#[cfg(not(target_arch = "wasm32"))]
impl IdbCacheStore {
    pub async fn open(
        _name: impl Into<String>,
        _block_size: u32,
        _total_blocks: u64,
    ) -> GibbloxResult<Self> {
        Err(GibbloxError::with_message(
            GibbloxErrorKind::Unsupported,
            "IdbCacheStore is only available on wasm32",
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl CacheStore for IdbCacheStore {
    fn block_size(&self) -> u32 {
        0
    }

    fn total_blocks(&self) -> u64 {
        0
    }

    async fn read_block(&self, _block_idx: u64, _out: &mut [u8]) -> GibbloxResult<bool> {
        Err(GibbloxError::with_message(
            GibbloxErrorKind::Unsupported,
            "IdbCacheStore is only available on wasm32",
        ))
    }

    async fn write_blocks(&self, _start_block: u64, _data: &[u8]) -> GibbloxResult<()> {
        Err(GibbloxError::with_message(
            GibbloxErrorKind::Unsupported,
            "IdbCacheStore is only available on wasm32",
        ))
    }
}
