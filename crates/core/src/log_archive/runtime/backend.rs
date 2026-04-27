// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

use std::any::Any;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::sync::{Arc, mpsc};
#[cfg(target_os = "linux")]
use std::thread;

use super::common::{
    ArchiveRecorderError, ArchiveRecorderStats, AsyncIoBackend, EffectiveAsyncIoBackend,
};

#[derive(Debug)]
pub(super) struct BlockingIoBackend;

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct PendingWrite {
    fd: RawFd,
    offset: u64,
    written: usize,
    kind: PendingWriteKind,
    operation: &'static str,
    path: PathBuf,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
enum PendingWriteKind {
    Contiguous {
        buffer: Box<[u8]>,
    },
    Vectored {
        owned_prefixes: Vec<Box<[u8]>>,
        external_ptr: *const u8,
        external_len: usize,
        owned_suffix: Option<Box<[u8]>>,
        iovecs: Box<[libc::iovec]>,
        total_len: usize,
        _owner: Box<dyn Any>,
    },
}

#[cfg(target_os = "linux")]
pub(super) struct IoUringBackend {
    ring: Arc<io_uring::IoUring>,
    completion_rx: Option<mpsc::Receiver<IoUringCompletion>>,
    completion_worker: Option<thread::JoinHandle<()>>,
    queue_depth: u32,
    submit_batch_max: u32,
    cqe_batch_max: u32,
    register_files_requested: bool,
    registered_file_slots: BTreeMap<RawFd, u32>,
    pending_writes: BTreeMap<u64, PendingWrite>,
    direct_completions: BTreeMap<u64, i32>,
    pending_submit_count: u32,
    next_user_data: u64,
    next_direct_user_data: u64,
    stats: IoUringBackendStats,
}

#[cfg(target_os = "linux")]
const IO_URING_SHUTDOWN_USER_DATA: u64 = u64::MAX;
#[cfg(target_os = "linux")]
const IO_URING_DIRECT_USER_DATA_START: u64 = u64::MAX / 2;

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
struct IoUringCompletion {
    user_data: u64,
    result: i32,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, Default)]
struct IoUringBackendStats {
    enqueued_writes: u64,
    submit_calls: u64,
    submitted_write_sqes: u64,
    completed_writes: u64,
    wait_calls: u64,
    pending_high_watermark: u64,
}

#[cfg(target_os = "linux")]
impl core::fmt::Debug for IoUringBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("IoUringBackend")
            .field("queue_depth", &self.queue_depth)
            .field("submit_batch_max", &self.submit_batch_max)
            .field("cqe_batch_max", &self.cqe_batch_max)
            .field("register_files_requested", &self.register_files_requested)
            .field("registered_file_slots", &self.registered_file_slots)
            .field("pending_writes", &self.pending_writes.len())
            .field("pending_submit_count", &self.pending_submit_count)
            .finish()
    }
}

/// Unified recorder I/O backend abstraction.
#[derive(Debug)]
pub(super) enum RecorderIoBackend {
    Blocking(BlockingIoBackend),
    #[cfg(target_os = "linux")]
    IoUring(Box<IoUringBackend>),
}

pub(super) struct ExternalPayloadWrite<'a> {
    pub path: &'a Path,
    pub offset: u64,
    pub owned_prefixes: Vec<Box<[u8]>>,
    pub external_ptr: *const u8,
    pub external_len: usize,
    pub owned_suffix: Option<Box<[u8]>>,
    pub owner: Box<dyn Any>,
    pub operation: &'static str,
}

impl RecorderIoBackend {
    pub(super) fn create(
        requested: AsyncIoBackend,
        io_uring_queue_depth: u32,
        io_submit_batch_max: u32,
        io_cqe_batch_max: u32,
        io_uring_register_files: bool,
    ) -> Result<(Self, EffectiveAsyncIoBackend), ArchiveRecorderError> {
        match requested {
            AsyncIoBackend::Blocking => Ok((
                Self::Blocking(BlockingIoBackend),
                EffectiveAsyncIoBackend::Blocking,
            )),
            AsyncIoBackend::IoUringPreferred => {
                #[cfg(target_os = "linux")]
                {
                    if let Ok(backend) = IoUringBackend::new(
                        io_uring_queue_depth,
                        io_submit_batch_max,
                        io_cqe_batch_max,
                        io_uring_register_files,
                    ) {
                        return Ok((
                            Self::IoUring(Box::new(backend)),
                            EffectiveAsyncIoBackend::IoUring,
                        ));
                    }
                }

                Ok((
                    Self::Blocking(BlockingIoBackend),
                    EffectiveAsyncIoBackend::Blocking,
                ))
            }
            AsyncIoBackend::IoUringRequired => {
                #[cfg(target_os = "linux")]
                {
                    IoUringBackend::new(
                        io_uring_queue_depth,
                        io_submit_batch_max,
                        io_cqe_batch_max,
                        io_uring_register_files,
                    )
                    .map(|backend| {
                        (
                            Self::IoUring(Box::new(backend)),
                            EffectiveAsyncIoBackend::IoUring,
                        )
                    })
                    .map_err(|_| {
                        ArchiveRecorderError::InvalidConfiguration(
                            "io_uring backend required but unavailable",
                        )
                    })
                }

                #[cfg(not(target_os = "linux"))]
                {
                    Err(ArchiveRecorderError::InvalidConfiguration(
                        "io_uring backend required but unavailable",
                    ))
                }
            }
        }
    }

    pub(super) fn refresh_registered_files(
        &mut self,
        #[cfg(target_os = "linux")] fds: &[RawFd],
        #[cfg(not(target_os = "linux"))] _fds: &[i32],
    ) -> Result<(), ArchiveRecorderError> {
        match self {
            Self::Blocking(_) => Ok(()),
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => backend.refresh_registered_files(fds),
        }
    }

    pub(super) fn flush_pending(&mut self) -> Result<(), ArchiveRecorderError> {
        match self {
            Self::Blocking(_) => Ok(()),
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => backend.flush_pending(),
        }
    }

    pub(super) fn write_all_at(
        &mut self,
        file: &mut File,
        path: &Path,
        offset: u64,
        bytes: &[u8],
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        match self {
            Self::Blocking(_) => file
                .seek(SeekFrom::Start(offset))
                .and_then(|_| file.write_all(bytes))
                .map_err(|source| ArchiveRecorderError::Io {
                    operation,
                    path: path.to_path_buf(),
                    source,
                }),
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => backend.enqueue_write(file, path, offset, bytes, operation),
        }
    }

    pub(super) fn write_owned_at(
        &mut self,
        file: &mut File,
        path: &Path,
        offset: u64,
        bytes: Box<[u8]>,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        match self {
            Self::Blocking(_) => file
                .seek(SeekFrom::Start(offset))
                .and_then(|_| file.write_all(&bytes))
                .map_err(|source| ArchiveRecorderError::Io {
                    operation,
                    path: path.to_path_buf(),
                    source,
                }),
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => {
                backend.enqueue_owned_write(file, path, offset, bytes, operation)
            }
        }
    }

    pub(super) unsafe fn write_vectored_external_at(
        &mut self,
        file: &mut File,
        write: ExternalPayloadWrite<'_>,
    ) -> Result<(), ArchiveRecorderError> {
        match self {
            Self::Blocking(_) => {
                file.seek(SeekFrom::Start(write.offset)).map_err(|source| {
                    ArchiveRecorderError::Io {
                        operation: write.operation,
                        path: write.path.to_path_buf(),
                        source,
                    }
                })?;
                for prefix in &write.owned_prefixes {
                    file.write_all(prefix)
                        .map_err(|source| ArchiveRecorderError::Io {
                            operation: write.operation,
                            path: write.path.to_path_buf(),
                            source,
                        })?;
                }
                let payload =
                    unsafe { core::slice::from_raw_parts(write.external_ptr, write.external_len) };
                file.write_all(payload)
                    .map_err(|source| ArchiveRecorderError::Io {
                        operation: write.operation,
                        path: write.path.to_path_buf(),
                        source,
                    })?;
                if let Some(suffix) = &write.owned_suffix {
                    file.write_all(suffix)
                        .map_err(|source| ArchiveRecorderError::Io {
                            operation: write.operation,
                            path: write.path.to_path_buf(),
                            source,
                        })?;
                }
                drop(write.owner);
                Ok(())
            }
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => unsafe {
                backend.enqueue_vectored_external_write(file, write)
            },
        }
    }

    pub(super) fn flush(
        &mut self,
        file: &mut File,
        path: &Path,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        self.flush_pending()?;
        file.flush().map_err(|source| ArchiveRecorderError::Io {
            operation,
            path: path.to_path_buf(),
            source,
        })
    }

    pub(super) fn sync_data(
        &mut self,
        file: &mut File,
        path: &Path,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        self.flush_pending()?;
        match self {
            Self::Blocking(_) => file.sync_data().map_err(|source| ArchiveRecorderError::Io {
                operation,
                path: path.to_path_buf(),
                source,
            }),
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => backend.sync_data(file, path, operation),
        }
    }

    pub(super) fn set_len(
        &mut self,
        file: &mut File,
        path: &Path,
        len: u64,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        self.flush_pending()?;
        file.set_len(len)
            .map_err(|source| ArchiveRecorderError::Io {
                operation,
                path: path.to_path_buf(),
                source,
            })
    }

    pub(super) fn accumulate_stats(&self, stats: &mut ArchiveRecorderStats) {
        match self {
            Self::Blocking(_) => {}
            #[cfg(target_os = "linux")]
            Self::IoUring(backend) => backend.accumulate_stats(stats),
        }
    }
}

#[cfg(target_os = "linux")]
impl IoUringBackend {
    fn new(
        queue_depth: u32,
        submit_batch_max: u32,
        cqe_batch_max: u32,
        register_files_requested: bool,
    ) -> std::io::Result<Self> {
        let queue_depth = queue_depth.max(1);
        let submit_batch_max = submit_batch_max.max(1).min(queue_depth);
        let cqe_batch_max = cqe_batch_max.max(1).min(queue_depth.saturating_mul(2));
        let ring = Arc::new(io_uring::IoUring::new(queue_depth)?);
        let use_completion_worker = io_uring_completion_worker_enabled();
        let (completion_rx, completion_worker) = if use_completion_worker {
            let (completion_tx, completion_rx) = mpsc::channel();
            let completion_ring = Arc::clone(&ring);
            let completion_worker = thread::Builder::new()
                .name("iox2-log-archive-iouring-cq".to_string())
                .spawn(move || completion_worker_loop(completion_ring, completion_tx))?;
            (Some(completion_rx), Some(completion_worker))
        } else {
            (None, None)
        };

        Ok(Self {
            ring,
            completion_rx,
            completion_worker,
            queue_depth,
            submit_batch_max,
            cqe_batch_max,
            register_files_requested,
            registered_file_slots: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            direct_completions: BTreeMap::new(),
            pending_submit_count: 0,
            next_user_data: 1,
            next_direct_user_data: IO_URING_DIRECT_USER_DATA_START,
            stats: IoUringBackendStats::default(),
        })
    }

    fn accumulate_stats(&self, stats: &mut ArchiveRecorderStats) {
        stats.async_write_enqueued = stats
            .async_write_enqueued
            .saturating_add(self.stats.enqueued_writes);
        stats.io_uring_submit_calls = stats
            .io_uring_submit_calls
            .saturating_add(self.stats.submit_calls);
        stats.io_uring_submitted_writes = stats
            .io_uring_submitted_writes
            .saturating_add(self.stats.submitted_write_sqes);
        stats.io_uring_completed_writes = stats
            .io_uring_completed_writes
            .saturating_add(self.stats.completed_writes);
        stats.io_uring_wait_calls = stats
            .io_uring_wait_calls
            .saturating_add(self.stats.wait_calls);
        stats.io_uring_pending_high_watermark = stats
            .io_uring_pending_high_watermark
            .max(self.stats.pending_high_watermark);
    }

    fn enqueue_write(
        &mut self,
        file: &File,
        path: &Path,
        offset: u64,
        bytes: &[u8],
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        self.enqueue_owned_write(
            file,
            path,
            offset,
            bytes.to_vec().into_boxed_slice(),
            operation,
        )
    }

    fn enqueue_owned_write(
        &mut self,
        file: &File,
        path: &Path,
        offset: u64,
        bytes: Box<[u8]>,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        if bytes.is_empty() {
            return Ok(());
        }

        let user_data = self.next_user_data;
        self.next_user_data = self.next_user_data.wrapping_add(1);
        let pending = PendingWrite {
            fd: file.as_raw_fd(),
            offset,
            written: 0,
            kind: PendingWriteKind::Contiguous { buffer: bytes },
            operation,
            path: path.to_path_buf(),
        };
        self.pending_writes.insert(user_data, pending);
        self.record_enqueued_write();
        self.push_write_entry(user_data)?;

        if self.pending_submit_count >= self.submit_batch_max {
            self.submit_pending()?;
        }

        if self.pending_writes.len() >= self.queue_depth as usize {
            self.submit_pending()?;
            self.wait_for_capacity()?;
        } else {
            self.reap_completed()?;
        }

        Ok(())
    }

    unsafe fn enqueue_vectored_external_write(
        &mut self,
        file: &File,
        write: ExternalPayloadWrite<'_>,
    ) -> Result<(), ArchiveRecorderError> {
        let total_len = write
            .owned_prefixes
            .iter()
            .map(|buffer| buffer.len())
            .sum::<usize>()
            + write.external_len
            + write.owned_suffix.as_ref().map_or(0, |buffer| buffer.len());
        if total_len == 0 {
            return Ok(());
        }

        let user_data = self.next_user_data;
        self.next_user_data = self.next_user_data.wrapping_add(1);
        let iovecs = build_iovecs(
            &write.owned_prefixes,
            write.external_ptr,
            write.external_len,
            write.owned_suffix.as_deref(),
            0,
        );
        let pending = PendingWrite {
            fd: file.as_raw_fd(),
            offset: write.offset,
            written: 0,
            kind: PendingWriteKind::Vectored {
                owned_prefixes: write.owned_prefixes,
                external_ptr: write.external_ptr,
                external_len: write.external_len,
                owned_suffix: write.owned_suffix,
                iovecs,
                total_len,
                _owner: write.owner,
            },
            operation: write.operation,
            path: write.path.to_path_buf(),
        };
        self.pending_writes.insert(user_data, pending);
        self.record_enqueued_write();
        self.push_write_entry(user_data)?;

        if self.pending_submit_count >= self.submit_batch_max {
            self.submit_pending()?;
        }

        if self.pending_writes.len() >= self.queue_depth as usize {
            self.submit_pending()?;
            self.wait_for_capacity()?;
        } else {
            self.reap_completed()?;
        }

        Ok(())
    }

    fn refresh_registered_files(&mut self, fds: &[RawFd]) -> Result<(), ArchiveRecorderError> {
        if !self.register_files_requested {
            self.registered_file_slots.clear();
            return Ok(());
        }

        self.flush_pending()?;

        let mut unique_fds = Vec::<RawFd>::new();
        for fd in fds {
            if !unique_fds.contains(fd) {
                unique_fds.push(*fd);
            }
        }

        let submitter = self.ring.submitter();
        if !self.registered_file_slots.is_empty() {
            let _ = submitter.unregister_files();
            self.registered_file_slots.clear();
        }
        if unique_fds.is_empty() {
            return Ok(());
        }

        submitter
            .register_files(&unique_fds)
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "register io_uring files",
                path: PathBuf::from("<io_uring>"),
                source,
            })?;
        self.registered_file_slots = unique_fds
            .into_iter()
            .enumerate()
            .map(|(index, fd)| (fd, index as u32))
            .collect();

        Ok(())
    }

    fn flush_pending(&mut self) -> Result<(), ArchiveRecorderError> {
        self.submit_pending()?;
        while !self.pending_writes.is_empty() {
            self.wait_for_and_reap(1)?;
        }
        Ok(())
    }

    fn sync_data(
        &mut self,
        file: &File,
        path: &Path,
        operation: &'static str,
    ) -> Result<(), ArchiveRecorderError> {
        self.flush_pending()?;
        self.submit_pending()?;

        let entry = if let Some(index) = self.registered_file_slots.get(&file.as_raw_fd()) {
            io_uring::opcode::Fsync::new(io_uring::types::Fixed(*index))
                .build()
                .user_data(0xFFFF_FFFF_FFFF_FFFE)
        } else {
            io_uring::opcode::Fsync::new(io_uring::types::Fd(file.as_raw_fd()))
                .build()
                .user_data(0xFFFF_FFFF_FFFF_FFFE)
        };
        let result = unsafe { self.submit_direct_and_wait_one(entry) }.map_err(|source| {
            ArchiveRecorderError::Io {
                operation,
                path: path.to_path_buf(),
                source,
            }
        })?;
        if result < 0 {
            return Err(ArchiveRecorderError::Io {
                operation,
                path: path.to_path_buf(),
                source: std::io::Error::from_raw_os_error(-result),
            });
        }
        Ok(())
    }

    fn push_write_entry(&mut self, user_data: u64) -> Result<(), ArchiveRecorderError> {
        loop {
            let entry = self.build_write_entry(user_data)?;
            let mut sq = unsafe { self.ring.submission_shared() };
            match unsafe { sq.push(&entry) } {
                Ok(()) => {
                    self.pending_submit_count += 1;
                    return Ok(());
                }
                Err(_) => {
                    drop(sq);
                    self.submit_pending()?;
                    self.wait_for_and_reap(1)?;
                }
            }
        }
    }

    fn build_write_entry(
        &self,
        user_data: u64,
    ) -> Result<io_uring::squeue::Entry, ArchiveRecorderError> {
        let pending = self.pending_writes.get(&user_data).ok_or({
            ArchiveRecorderError::RecoveryInconsistent("missing io_uring pending write state")
        })?;
        let offset = pending.offset + pending.written as u64;
        let entry = match &pending.kind {
            PendingWriteKind::Contiguous { buffer } => {
                let remaining = buffer.len().checked_sub(pending.written).ok_or(
                    ArchiveRecorderError::RecoveryInconsistent("io_uring pending write underflow"),
                )?;
                let ptr = buffer[pending.written..].as_ptr();
                if let Some(index) = self.registered_file_slots.get(&pending.fd) {
                    io_uring::opcode::Write::new(
                        io_uring::types::Fixed(*index),
                        ptr,
                        remaining as _,
                    )
                    .offset(offset)
                    .build()
                    .user_data(user_data)
                } else {
                    io_uring::opcode::Write::new(
                        io_uring::types::Fd(pending.fd),
                        ptr,
                        remaining as _,
                    )
                    .offset(offset)
                    .build()
                    .user_data(user_data)
                }
            }
            PendingWriteKind::Vectored { iovecs, .. } => {
                if let Some(index) = self.registered_file_slots.get(&pending.fd) {
                    io_uring::opcode::Writev::new(
                        io_uring::types::Fixed(*index),
                        iovecs.as_ptr(),
                        iovecs.len() as _,
                    )
                    .offset(offset)
                    .build()
                    .user_data(user_data)
                } else {
                    io_uring::opcode::Writev::new(
                        io_uring::types::Fd(pending.fd),
                        iovecs.as_ptr(),
                        iovecs.len() as _,
                    )
                    .offset(offset)
                    .build()
                    .user_data(user_data)
                }
            }
        };
        Ok(entry)
    }

    fn submit_pending(&mut self) -> Result<(), ArchiveRecorderError> {
        if self.pending_submit_count == 0 {
            return Ok(());
        }
        let submitted = self.pending_submit_count as u64;
        self.ring
            .submit()
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "submit io_uring write batch",
                path: PathBuf::from("<io_uring>"),
                source,
            })?;
        self.stats.submit_calls = self.stats.submit_calls.saturating_add(1);
        self.stats.submitted_write_sqes = self.stats.submitted_write_sqes.saturating_add(submitted);
        self.pending_submit_count = 0;
        Ok(())
    }

    fn wait_for_and_reap(&mut self, min_completions: usize) -> Result<(), ArchiveRecorderError> {
        self.stats.wait_calls = self.stats.wait_calls.saturating_add(1);
        if self.completion_rx.is_none() {
            self.ring
                .submit_and_wait(min_completions)
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "wait for io_uring completion",
                    path: PathBuf::from("<io_uring>"),
                    source,
                })?;
            self.reap_completed()?;
            return Ok(());
        }

        let mut completed = self.reap_completed()?;
        while completed < min_completions {
            let completion = self
                .completion_rx
                .as_ref()
                .expect("completion worker receiver must exist")
                .recv()
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "wait for io_uring completion worker",
                    path: PathBuf::from("<io_uring>"),
                    source: Error::new(ErrorKind::BrokenPipe, source),
                })?;
            completed += self.handle_completion(completion)?;
            completed += self.reap_completed()?;
        }
        Ok(())
    }

    fn wait_for_capacity(&mut self) -> Result<(), ArchiveRecorderError> {
        let completion_target = self
            .cqe_batch_max
            .min(self.queue_depth)
            .saturating_div(2)
            .max(1) as usize;
        self.wait_for_and_reap(completion_target.min(self.pending_writes.len()))
    }

    fn reap_completed(&mut self) -> Result<usize, ArchiveRecorderError> {
        if self.completion_rx.is_none() {
            return self.reap_ring_completed();
        }

        let mut completed = 0usize;
        for _ in 0..self.cqe_batch_max {
            let completion = match self
                .completion_rx
                .as_ref()
                .expect("completion worker receiver must exist")
                .try_recv()
            {
                Ok(completion) => completion,
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(ArchiveRecorderError::Io {
                        operation: "read io_uring completion worker",
                        path: PathBuf::from("<io_uring>"),
                        source: Error::new(
                            ErrorKind::BrokenPipe,
                            "io_uring completion worker disconnected",
                        ),
                    });
                }
            };
            completed += self.handle_completion(completion)?;
        }

        Ok(completed)
    }

    fn reap_ring_completed(&mut self) -> Result<usize, ArchiveRecorderError> {
        let mut completed = 0usize;
        for _ in 0..self.cqe_batch_max {
            let completion = {
                let mut cq = unsafe { self.ring.completion_shared() };
                cq.next().map(|cqe| IoUringCompletion {
                    user_data: cqe.user_data(),
                    result: cqe.result(),
                })
            };
            let Some(completion) = completion else {
                break;
            };
            completed += self.handle_completion(completion)?;
        }

        Ok(completed)
    }

    fn handle_completion(
        &mut self,
        completion: IoUringCompletion,
    ) -> Result<usize, ArchiveRecorderError> {
        let IoUringCompletion { user_data, result } = completion;
        if user_data >= IO_URING_DIRECT_USER_DATA_START {
            self.direct_completions.insert(user_data, result);
            return Ok(0);
        }

        let mut pending = self.pending_writes.remove(&user_data).ok_or(
            ArchiveRecorderError::RecoveryInconsistent("missing io_uring completion state"),
        )?;
        if result < 0 {
            return Err(ArchiveRecorderError::Io {
                operation: pending.operation,
                path: pending.path,
                source: std::io::Error::from_raw_os_error(-result),
            });
        }
        if result == 0 {
            return Err(ArchiveRecorderError::Io {
                operation: pending.operation,
                path: pending.path,
                source: Error::new(ErrorKind::WriteZero, "io_uring write returned zero bytes"),
            });
        }

        pending.written += result as usize;
        let total_len = pending_total_len(&pending);
        if pending.written < total_len {
            refresh_pending_iovecs(&mut pending);
            self.pending_writes.insert(user_data, pending);
            self.push_write_entry(user_data)?;
            Ok(0)
        } else {
            self.stats.completed_writes = self.stats.completed_writes.saturating_add(1);
            Ok(1)
        }
    }

    fn next_direct_user_data(&mut self) -> u64 {
        let user_data = self.next_direct_user_data;
        self.next_direct_user_data =
            if self.next_direct_user_data == IO_URING_SHUTDOWN_USER_DATA - 1 {
                IO_URING_DIRECT_USER_DATA_START
            } else {
                self.next_direct_user_data + 1
            };
        user_data
    }

    fn submit_entry(&mut self, entry: io_uring::squeue::Entry) -> std::io::Result<()> {
        loop {
            let mut sq = unsafe { self.ring.submission_shared() };
            match unsafe { sq.push(&entry) } {
                Ok(()) => {
                    drop(sq);
                    self.ring.submit()?;
                    return Ok(());
                }
                Err(_) => {
                    drop(sq);
                    self.submit_pending().map_err(archive_to_io_error)?;
                    self.wait_for_and_reap(1).map_err(archive_to_io_error)?;
                }
            }
        }
    }

    fn wait_for_direct_completion(&mut self, user_data: u64) -> std::io::Result<i32> {
        loop {
            if let Some(result) = self.direct_completions.remove(&user_data) {
                return Ok(result);
            }
            if let Some(completion_rx) = self.completion_rx.as_ref() {
                let completion = completion_rx.recv().map_err(|source| {
                    Error::new(
                        ErrorKind::BrokenPipe,
                        format!("io_uring completion worker disconnected: {source}"),
                    )
                })?;
                self.handle_completion(completion)
                    .map_err(archive_to_io_error)?;
            } else {
                self.ring.submit_and_wait(1)?;
                self.reap_ring_completed().map_err(archive_to_io_error)?;
            }
        }
    }

    fn submit_shutdown(&mut self) -> std::io::Result<()> {
        let entry = io_uring::opcode::Nop::new()
            .build()
            .user_data(IO_URING_SHUTDOWN_USER_DATA);
        self.submit_entry(entry)
    }

    fn join_completion_worker(&mut self) {
        if let Some(worker) = self.completion_worker.take() {
            let _ = self.submit_shutdown();
            let _ = worker.join();
        }
    }

    unsafe fn submit_direct_and_wait_one(
        &mut self,
        entry: io_uring::squeue::Entry,
    ) -> std::io::Result<i32> {
        let user_data = self.next_direct_user_data();
        let entry = entry.user_data(user_data);
        self.submit_entry(entry)?;
        self.wait_for_direct_completion(user_data)
    }

    fn record_enqueued_write(&mut self) {
        self.stats.enqueued_writes = self.stats.enqueued_writes.saturating_add(1);
        self.stats.pending_high_watermark = self
            .stats
            .pending_high_watermark
            .max(self.pending_writes.len() as u64);
    }
}

#[cfg(target_os = "linux")]
impl Drop for IoUringBackend {
    fn drop(&mut self) {
        let _ = self.flush_pending();
        self.join_completion_worker();
    }
}

#[cfg(target_os = "linux")]
fn completion_worker_loop(
    ring: Arc<io_uring::IoUring>,
    completion_tx: mpsc::Sender<IoUringCompletion>,
) {
    loop {
        if ring.submitter().submit_and_wait(1).is_err() {
            return;
        }

        loop {
            let completion = {
                let mut cq = unsafe { ring.completion_shared() };
                cq.next().map(|cqe| IoUringCompletion {
                    user_data: cqe.user_data(),
                    result: cqe.result(),
                })
            };
            let Some(completion) = completion else {
                break;
            };

            if completion.user_data == IO_URING_SHUTDOWN_USER_DATA {
                return;
            }
            if completion_tx.send(completion).is_err() {
                return;
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn io_uring_completion_worker_enabled() -> bool {
    std::env::var("IOX2_LOG_ARCHIVE_IO_URING_COMPLETION_WORKER")
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn archive_to_io_error(error: ArchiveRecorderError) -> Error {
    match error {
        ArchiveRecorderError::Io { source, .. } => source,
        other => Error::other(format!("{other:?}")),
    }
}

#[cfg(target_os = "linux")]
fn pending_total_len(pending: &PendingWrite) -> usize {
    match &pending.kind {
        PendingWriteKind::Contiguous { buffer } => buffer.len(),
        PendingWriteKind::Vectored { total_len, .. } => *total_len,
    }
}

#[cfg(target_os = "linux")]
fn refresh_pending_iovecs(pending: &mut PendingWrite) {
    if let PendingWriteKind::Vectored {
        owned_prefixes,
        external_ptr,
        external_len,
        owned_suffix,
        iovecs,
        ..
    } = &mut pending.kind
    {
        *iovecs = build_iovecs(
            owned_prefixes,
            *external_ptr,
            *external_len,
            owned_suffix.as_deref(),
            pending.written,
        );
    }
}

#[cfg(target_os = "linux")]
fn build_iovecs(
    owned_prefixes: &[Box<[u8]>],
    external_ptr: *const u8,
    external_len: usize,
    owned_suffix: Option<&[u8]>,
    mut skip: usize,
) -> Box<[libc::iovec]> {
    let mut iovecs = Vec::new();
    for buffer in owned_prefixes {
        push_iovec(&mut iovecs, buffer.as_ptr(), buffer.len(), &mut skip);
    }
    push_iovec(&mut iovecs, external_ptr, external_len, &mut skip);
    if let Some(suffix) = owned_suffix {
        push_iovec(&mut iovecs, suffix.as_ptr(), suffix.len(), &mut skip);
    }
    iovecs.into_boxed_slice()
}

#[cfg(target_os = "linux")]
fn push_iovec(iovecs: &mut Vec<libc::iovec>, ptr: *const u8, len: usize, skip: &mut usize) {
    if len == 0 {
        return;
    }
    if *skip >= len {
        *skip -= len;
        return;
    }
    let offset = *skip;
    *skip = 0;
    iovecs.push(libc::iovec {
        iov_base: unsafe { ptr.add(offset) }.cast_mut().cast(),
        iov_len: len - offset,
    });
}
