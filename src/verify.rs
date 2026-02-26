use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result};
use zim::Zim;

pub const ZIM_MAGIC_NUMBER: u32 = 72_173_914;

#[derive(Debug, Clone)]
pub struct VerifyOptions {
    pub checksum: bool,
    pub tail_window_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub path: String,
    pub size_bytes: u64,
    pub magic_ok: bool,
    pub tail_all_zero: bool,
    pub tail_zero_ratio: f64,
    pub article_count: u32,
    pub cluster_count: u32,
    pub checksum_ok: Option<bool>,
}

pub fn verify_zim_file(path: &Path, options: &VerifyOptions) -> Result<VerifyReport> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open zim file {}", path.display()))?;

    let metadata = file
        .metadata()
        .with_context(|| format!("failed to stat zim file {}", path.display()))?;
    let size_bytes = metadata.len();
    if size_bytes < 80 {
        anyhow::bail!("zim file is too small to contain a valid header");
    }

    let mut magic_buf = [0_u8; 4];
    file.read_exact(&mut magic_buf)
        .context("failed reading zim magic bytes")?;
    let magic = u32::from_le_bytes(magic_buf);
    let magic_ok = magic == ZIM_MAGIC_NUMBER;
    if !magic_ok {
        anyhow::bail!(
            "invalid ZIM magic: expected {ZIM_MAGIC_NUMBER}, got {magic} (file likely corrupt or wrong format)"
        );
    }

    let tail_window_bytes = options.tail_window_bytes.max(64) as u64;
    let tail_read_len = tail_window_bytes.min(size_bytes) as usize;
    file.seek(SeekFrom::End(-(tail_read_len as i64)))
        .context("failed seeking to file tail")?;

    let mut tail = vec![0_u8; tail_read_len];
    file.read_exact(&mut tail)
        .context("failed reading file tail")?;

    let zero_count = tail.iter().filter(|byte| **byte == 0).count();
    let tail_zero_ratio = zero_count as f64 / tail.len() as f64;
    let tail_all_zero = zero_count == tail.len();
    if tail_all_zero {
        anyhow::bail!(
            "zim tail appears fully sparse/zeroed (last {} bytes): likely incomplete download",
            tail.len()
        );
    }

    let zim = Zim::new(path)
        .with_context(|| format!("failed parsing ZIM structures for {}", path.display()))?;

    let checksum_ok = if options.checksum {
        match zim.verify_checksum() {
            Ok(()) => Some(true),
            Err(error) => {
                anyhow::bail!("ZIM checksum verification failed: {error}");
            }
        }
    } else {
        None
    };

    Ok(VerifyReport {
        path: path.display().to_string(),
        size_bytes,
        magic_ok,
        tail_all_zero,
        tail_zero_ratio,
        article_count: zim.header.article_count,
        cluster_count: zim.header.cluster_count,
        checksum_ok,
    })
}
