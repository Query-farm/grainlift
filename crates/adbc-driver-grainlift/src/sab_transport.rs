// Copyright (c) 2026 ADBC Drivers Contributors
// Copyright (c) 2026 Query Farm LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `iroh://` byte streams inside DuckDB-WASM (Haybarn, cross-origin isolated).
//!
//! A browser page cannot open QUIC sockets from the DuckDB worker. Haybarn
//! instead lets the page own one Iroh node in an adapter Worker
//! (`@query-farm/vgi-rpc-iroh-browser`, installed through Haybarn's
//! `irohAdapterWorker` bridge option). The adapter serves SharedArrayBuffer
//! ring "slots" carved out of DuckDB's shared linear memory: each claimed slot
//! is one `vgi-rpc/arrow-mux/1` stream to the remote endpoint, which is exactly
//! the protocol grainlift-server's Iroh listener speaks.
//!
//! The ring ABI (header/slot layout and the `vgi_wasm_*` JS stubs compiled into
//! the Haybarn engine) is shared with the VGI extension; see
//! `vgi/src/include/vgi_sab_abi.hpp` and `haybarn-wasm/lib/js-stubs.js`.

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use vgi_rpc_client::Transport;

unsafe extern "C" {
    fn vgi_wasm_ensure_worker(location: *const c_char, region_offset: i32) -> i32;
    fn vgi_wasm_slot_open(location: *const c_char, region_offset: i32) -> i32;
    fn vgi_wasm_slot_write(region_offset: i32, slot: i32, data: *const u8, n: i32) -> i32;
    fn vgi_wasm_slot_write_eos(region_offset: i32, slot: i32);
    fn vgi_wasm_slot_read(region_offset: i32, slot: i32, data: *mut u8, n: i32) -> i32;
    fn vgi_wasm_slot_terminal_error(
        region_offset: i32,
        slot: i32,
        code: *mut i32,
        detail: *mut i32,
    ) -> i32;
    fn vgi_wasm_slot_release(region_offset: i32, slot: i32);
    fn vgi_wasm_set_channel(region_offset: i32);
}

// ---- ABI v1 layout (i32 lanes) --------------------------------------------
const MAGIC: i32 = 0x4253_4756; // 'VGSB'
const VERSION: i32 = 1;
const HDR_MAGIC: usize = 0;
const HDR_VERSION: usize = 1;
const HDR_N_SLOTS: usize = 2;
const HDR_RING_CAP: usize = 3;
const HDR_SLOT_STRIDE: usize = 4;
const HDR_SLOTS_OFF: usize = 5;
const HDR_FEATURES: usize = 6;
const HEADER_BYTES: usize = 64;
const SLOT_CONTROL_BYTES: usize = 64;
const FEATURE_TERMINAL_ERROR: i32 = 1;
const WOULD_BLOCK: i32 = -2;
const TERMINAL_TRANSPORT_ERROR: i32 = -3;

// A grainlift connection holds a control stream plus one stream per open
// result or bind, and ATTACH keeps a small connection pool, so give each
// target more slots than VGI's default of four.
const SLOTS_PER_REGION: usize = 16;
const RING_CAP: usize = 64 * 1024;

fn slot_stride() -> usize {
    (SLOT_CONTROL_BYTES + 2 * RING_CAP + 63) & !63
}

/// One ABI-v1 region per canonical target, allocated once and kept for the
/// life of the process (the adapter Worker keeps a view of it).
fn region_for(target: &str) -> io::Result<i32> {
    static REGIONS: OnceLock<Mutex<HashMap<String, i32>>> = OnceLock::new();
    let regions = REGIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut regions = regions
        .lock()
        .map_err(|_| io::Error::other("Iroh SAB region table is poisoned"))?;
    if let Some(offset) = regions.get(target) {
        return Ok(*offset);
    }
    let bytes = HEADER_BYTES + SLOTS_PER_REGION * slot_stride();
    let layout = std::alloc::Layout::from_size_align(bytes, 64)
        .map_err(|error| io::Error::other(error.to_string()))?;
    // SAFETY: non-zero size; the region is intentionally leaked.
    let base = unsafe { std::alloc::alloc_zeroed(layout) };
    if base.is_null() {
        return Err(io::Error::other("could not allocate the Iroh SAB region"));
    }
    let offset = i32::try_from(base as usize)
        .map_err(|_| io::Error::other("Iroh SAB region is outside wasm32 memory"))?;
    // SAFETY: `base` is 64-byte aligned and HEADER_BYTES long at least.
    let lanes = unsafe { std::slice::from_raw_parts(base.cast::<AtomicI32>(), HEADER_BYTES / 4) };
    lanes[HDR_VERSION].store(VERSION, Ordering::SeqCst);
    lanes[HDR_N_SLOTS].store(SLOTS_PER_REGION as i32, Ordering::SeqCst);
    lanes[HDR_RING_CAP].store(RING_CAP as i32, Ordering::SeqCst);
    lanes[HDR_SLOT_STRIDE].store(slot_stride() as i32, Ordering::SeqCst);
    lanes[HDR_SLOTS_OFF].store(HEADER_BYTES as i32, Ordering::SeqCst);
    lanes[HDR_FEATURES].store(FEATURE_TERMINAL_ERROR, Ordering::SeqCst);
    lanes[HDR_MAGIC].store(MAGIC, Ordering::SeqCst);
    regions.insert(target.to_string(), offset);
    Ok(offset)
}

struct SlotReader {
    region: i32,
    slot: i32,
    timeout: Duration,
}

struct SlotWriter {
    region: i32,
    slot: i32,
}

impl Read for SlotReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let len = i32::try_from(buf.len()).unwrap_or(i32::MAX);
        let started = Instant::now();
        loop {
            // The JS ring stubs keep the channel offset per realm; DuckDB may
            // run this read on any of its pthreads.
            let n = unsafe {
                vgi_wasm_set_channel(self.region);
                vgi_wasm_slot_read(self.region, self.slot, buf.as_mut_ptr(), len)
            };
            match n {
                n if n > 0 => return Ok(n as usize),
                0 => return Ok(0),
                WOULD_BLOCK => {
                    if started.elapsed() >= self.timeout {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "timed out waiting for the Iroh adapter Worker",
                        ));
                    }
                }
                TERMINAL_TRANSPORT_ERROR => {
                    let (mut code, mut detail) = (0i32, 0i32);
                    let known = unsafe {
                        vgi_wasm_slot_terminal_error(self.region, self.slot, &mut code, &mut detail)
                    };
                    return Err(io::Error::other(if known == 1 {
                        format!(
                            "Iroh transport failed in the adapter Worker (code {code}, detail {detail})"
                        )
                    } else {
                        "Iroh transport failed in the adapter Worker".to_string()
                    }));
                }
                other => {
                    return Err(io::Error::other(format!("Iroh SAB read failed ({other})")));
                }
            }
        }
    }
}

impl Write for SlotWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let len = i32::try_from(buf.len()).unwrap_or(i32::MAX);
        let n = unsafe {
            vgi_wasm_set_channel(self.region);
            vgi_wasm_slot_write(self.region, self.slot, buf.as_ptr(), len)
        };
        if n < 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("Iroh SAB write failed ({n})"),
            ));
        }
        Ok(n as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Bring up the page's adapter Worker for `target` without claiming a slot.
///
/// The spawn request is a `postMessage` to the page, which only reaches
/// Haybarn's bridge from DuckDB's main worker thread, not from its pthread
/// pool. The host calls this while binding a query (main thread) so later
/// slot claims from any pthread find the Worker ready.
pub fn prepare(target: &str) -> io::Result<()> {
    let region = region_for(target)?;
    let location = CString::new(target).map_err(|_| io::Error::other("invalid Iroh target"))?;
    let ready = unsafe {
        vgi_wasm_set_channel(region);
        vgi_wasm_ensure_worker(location.as_ptr(), region)
    };
    if ready != 0 {
        return Err(io::Error::other(
            "the page's Iroh adapter Worker is not available",
        ));
    }
    Ok(())
}

/// One claimed ring slot: a duplex `arrow-mux/1` stream to the remote endpoint.
pub struct SabTransport {
    reader: SlotReader,
    writer: SlotWriter,
    open: bool,
}

impl SabTransport {
    /// Claim a slot for `target` (a canonical `iroh://<EndpointId>`).
    pub fn open(target: &str, timeout: Duration) -> io::Result<Self> {
        let region = region_for(target)?;
        let location = CString::new(target).map_err(|_| io::Error::other("invalid Iroh target"))?;
        unsafe { vgi_wasm_set_channel(region) };
        if unsafe { vgi_wasm_ensure_worker(location.as_ptr(), region) } != 0 {
            return Err(io::Error::other(
                "iroh:// needs Haybarn's cross-origin-isolated (COI) build with an Iroh adapter Worker \
                 installed by the page (installVgiWebWorkerBridge({ irohAdapterWorker }))",
            ));
        }
        let slot = unsafe { vgi_wasm_slot_open(location.as_ptr(), region) };
        if slot < 0 {
            return Err(io::Error::other(
                "all Iroh SAB slots for this endpoint are in use",
            ));
        }
        Ok(Self {
            reader: SlotReader {
                region,
                slot,
                timeout,
            },
            writer: SlotWriter { region, slot },
            open: true,
        })
    }

    fn shutdown(&mut self) {
        if self.open {
            self.open = false;
            unsafe {
                vgi_wasm_set_channel(self.writer.region);
                vgi_wasm_slot_write_eos(self.writer.region, self.writer.slot);
                vgi_wasm_slot_release(self.writer.region, self.writer.slot);
            }
        }
    }
}

impl Transport for SabTransport {
    fn split(&mut self) -> (&mut dyn Read, &mut dyn Write) {
        (&mut self.reader, &mut self.writer)
    }

    fn is_reusable(&self) -> bool {
        self.open
    }

    fn close(&mut self) -> vgi_rpc_client::Result<()> {
        self.shutdown();
        Ok(())
    }
}

impl Drop for SabTransport {
    fn drop(&mut self) {
        self.shutdown();
    }
}
