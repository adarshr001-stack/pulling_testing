use anyhow::{Context, Result};
use dotenv::dotenv;
use ssh2::Session;
use ssh_key::{private::RsaKeypair, LineEnding, PrivateKey};
use std::collections::{HashSet, VecDeque};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

// Monitoring
use sysinfo::System;

#[derive(Debug, Clone)]
struct Config {
    bank_host: String,
    bank_port: u16,
    bank_username: String,
    bank_private_key_path: Option<String>,
    bank_private_key_content: Option<String>,
    bank_remote_path: String,

    local_storage_path: String, // Local directory to store downloaded files

    upload_chunk_size: usize, // e.g., 10 * 1024 * 1024 for 10MB (used for read buffer)
    max_concurrent_uploads: usize, // Number of parallel downloads
}

impl Config {
    fn from_env() -> Result<Self> {
        dotenv().ok();

        Ok(Self {
            bank_host: env::var("BANK_HOST").context("BANK_HOST missing")?,
            bank_port: env::var("BANK_PORT")
                .context("BANK_PORT missing")?
                .parse()
                .context("BANK_PORT parse failed")?,
            bank_username: env::var("BANK_USERNAME").context("BANK_USERNAME missing")?,
            bank_private_key_path: env::var("BANK_PRIVATE_KEY_PATH").ok(),
            bank_private_key_content: env::var("BANK_PRIVATE_KEY_CONTENT").ok(),
            bank_remote_path: env::var("BANK_REMOTE_PATH").context("BANK_REMOTE_PATH missing")?,
            local_storage_path: env::var("LOCAL_STORAGE_PATH").context("LOCAL_STORAGE_PATH missing")?,
            upload_chunk_size: env::var("UPLOAD_CHUNK_SIZE_MB")
                .unwrap_or_else(|_| "10".to_string())
                .parse::<usize>()
                .context("UPLOAD_CHUNK_SIZE_MB parse failed")? * 1024 * 1024,
            max_concurrent_uploads: env::var("MAX_CONCURRENT_UPLOADS")
                .unwrap_or_else(|_| "4".to_string())
                .parse::<usize>()
                .context("MAX_CONCURRENT_UPLOADS parse failed")?,
        })
    }
}

// Shared state to track file processing
struct FileProcessingState {
    files_in_progress: Mutex<HashSet<String>>,
    files_completed: Mutex<HashSet<String>>,
    files_failed: Mutex<HashSet<String>>,
    file_queue: Mutex<VecDeque<String>>,
}

impl FileProcessingState {
    fn new(files: Vec<String>) -> Self {
        Self {
            files_in_progress: Mutex::new(HashSet::new()),
            files_completed: Mutex::new(HashSet::new()),
            files_failed: Mutex::new(HashSet::new()),
            file_queue: Mutex::new(files.into_iter().collect()),
        }
    }

    // Try to claim a file for processing
    async fn claim_next_file(&self) -> Option<String> {
        let mut queue = self.file_queue.lock().await;
        let mut in_progress = self.files_in_progress.lock().await;

        if let Some(file) = queue.pop_front() {
            in_progress.insert(file.clone());
            Some(file)
        } else {
            None
        }
    }

    // Mark file as completed successfully
    async fn mark_completed(&self, file: &str) {
        let mut in_progress = self.files_in_progress.lock().await;
        let mut completed = self.files_completed.lock().await;

        in_progress.remove(file);
        completed.insert(file.to_string());
    }

    // Mark file as failed
    async fn mark_failed(&self, file: &str) {
        let mut in_progress = self.files_in_progress.lock().await;
        let mut failed = self.files_failed.lock().await;

        in_progress.remove(file);
        failed.insert(file.to_string());
    }

    // Get statistics
    async fn get_stats(&self) -> (usize, usize, usize, usize) {
        let queue = self.file_queue.lock().await;
        let in_progress = self.files_in_progress.lock().await;
        let completed = self.files_completed.lock().await;
        let failed = self.files_failed.lock().await;

        (queue.len(), in_progress.len(), completed.len(), failed.len())
    }
}

// Global metrics tracked atomically
#[derive(Debug)]
struct Metrics {
    total_bytes: AtomicU64,
    files_completed: AtomicU64,
    files_failed: AtomicU64,
    total_time_ns: AtomicU64, // total file-processing time accumulated (nanoseconds)
    start_instant: Instant,
}

impl Metrics {
    fn new() -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            files_completed: AtomicU64::new(0),
            files_failed: AtomicU64::new(0),
            total_time_ns: AtomicU64::new(0),
            start_instant: Instant::now(),
        }
    }

    fn add_bytes(&self, n: u64) {
        self.total_bytes.fetch_add(n, Ordering::Relaxed);
    }

    fn inc_completed(&self) {
        self.files_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failed(&self) {
        self.files_failed.fetch_add(1, Ordering::Relaxed);
    }

    fn add_time_ns(&self, ns: u64) {
        self.total_time_ns.fetch_add(ns, Ordering::Relaxed);
    }

    fn elapsed(&self) -> Duration {
        self.start_instant.elapsed()
    }

    fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.total_bytes.load(Ordering::Relaxed),
            self.files_completed.load(Ordering::Relaxed),
            self.files_failed.load(Ordering::Relaxed),
            self.total_time_ns.load(Ordering::Relaxed),
        )
    }
}

const MONITOR_INTERVAL_SECS: u64 = 5;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    println!("PROCESS ID: {}", process::id());
    let cfg = Config::from_env()?;

    println!("Loaded config: {:?}", cfg);
    println!("Max concurrent uploads: {}", cfg.max_concurrent_uploads);

    // Ensure local storage directory exists
    fs::create_dir_all(&cfg.local_storage_path)
        .context("Failed to create local storage directory")?;
    println!("Local storage directory: {}", cfg.local_storage_path);

    // Create initial SFTP session in blocking thread and list files recursively
    let cfg_clone_for_listing = cfg.clone();
    let all_files: Vec<String> = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
        // create session and list
        let sess = create_sftp_session_blocking(&cfg_clone_for_listing)?;
        let files = list_sftp_files_recursively(&sess, Path::new(&cfg_clone_for_listing.bank_remote_path))?;
        Ok(files)
    })
    .await??
    ;

    println!("Found {} files on SFTP server", all_files.len());

    if all_files.is_empty() {
        println!("No files to process.");
        return Ok(());
    }

    // Create shared state for tracking file processing
    let state = Arc::new(FileProcessingState::new(all_files));

    // Semaphore to limit concurrent downloads
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrent_uploads));

    // Metrics
    let metrics = Arc::new(Metrics::new());

    // Spawn monitoring task
    {
        let state_clone = Arc::clone(&state);
        let metrics_clone = Arc::clone(&metrics);
        let cfg_for_monitor = cfg.clone();

        tokio::spawn(async move {
            monitor_task(state_clone, metrics_clone, cfg_for_monitor).await;
        });
    }

    // Spawn worker tasks
    let mut handles = vec![];

    for worker_id in 0..cfg.max_concurrent_uploads {
        let cfg_clone = cfg.clone();
        let state_clone = Arc::clone(&state);
        let semaphore_clone = Arc::clone(&semaphore);
        let metrics_clone = Arc::clone(&metrics);

        let handle = tokio::spawn(async move {
            worker_task(worker_id, cfg_clone, state_clone, semaphore_clone, metrics_clone).await
        });

        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Worker task panicked: {}", e);
        }
    }

    // Print final statistics
    let (remaining, in_progress, completed_count, failed_count) = state.get_stats().await;
    println!("\n=== Final Statistics ===");
    println!("Completed files (tracked set len): {}", completed_count);
    println!("Failed files (tracked set len): {}", failed_count);
    println!("In Progress: {}", in_progress);
    println!("Remaining: {}", remaining);

    let (total_bytes, total_completed, total_failed, total_time_ns) = metrics.snapshot();
    println!("Global metrics summary:");
    println!("  Total bytes transferred: {} bytes", total_bytes);
    println!("  Files completed (counter): {}", total_completed);
    println!("  Files failed (counter): {}", total_failed);
    println!("  Total file-processing time (s): {:.3}", (total_time_ns as f64) / 1_000_000_000.0);

    if total_failed > 0 {
        println!("\n⚠️ Some files failed to upload. Check logs above for details.");
    } else {
        println!("\n✅ All files transferred successfully (or none failed according to metrics)!");
    }

    Ok(())
}

// Worker task that processes files from the queue
async fn worker_task(
    worker_id: usize,
    cfg: Config,
    state: Arc<FileProcessingState>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<Metrics>,
) {
    println!("[Worker {}] Started", worker_id);

    loop {
        // Try to get the next file to process
        let file_path = match state.claim_next_file().await {
            Some(path) => path,
            None => {
                println!("[Worker {}] No more files to process, shutting down", worker_id);
                break;
            }
        };

        // Acquire semaphore permit (limits concurrent downloads)
        let _permit = semaphore.acquire().await.unwrap();

        println!("[Worker {}] Processing file: {}", worker_id, file_path);

        // For safety with ssh2 (non-Send), do the entire connection + download in spawn_blocking
        let cfg_for_block = cfg.clone();
        let file_to_process = file_path.clone();

        let start = Instant::now();
        let download_result = tokio::task::spawn_blocking(move || -> Result<u64> {
            // create session
            let sess = create_sftp_session_blocking(&cfg_for_block)?;
            // perform download of file_to_process using sess
            blocking_download_file(&sess, &cfg_for_block, &file_to_process)
        })
        .await;

        match download_result {
            Ok(Ok(bytes_written)) => {
                let duration = start.elapsed();
                metrics.add_time_ns(duration.as_nanos() as u64);
                metrics.add_bytes(bytes_written);
                metrics.inc_completed();
                println!(
                    "[Worker {}] Successfully downloaded: {} ({} bytes, {:.3}s)",
                    worker_id,
                    file_path,
                    bytes_written,
                    duration.as_secs_f64()
                );
                state.mark_completed(&file_path).await;
            }
            Ok(Err(e)) => {
                let duration = start.elapsed();
                metrics.add_time_ns(duration.as_nanos() as u64);
                metrics.inc_failed();
                eprintln!(
                    "[Worker {}]  Failed to download {}: {} (after {:.3}s)",
                    worker_id,
                    file_path,
                    e,
                    duration.as_secs_f64()
                );
                // optionally push back to queue or mark failed
                state.mark_failed(&file_path).await;
            }
            Err(join_err) => {
                metrics.inc_failed();
                eprintln!(
                    "[Worker {}] ❌ Blocking task panicked or was cancelled for {}: {}",
                    worker_id, file_path, join_err
                );
                state.mark_failed(&file_path).await;
            }
        }

        // Print current statistics
        let (remaining, in_progress, completed, failed) = state.get_stats().await;
        println!(
            "[Worker {}] Progress: Remaining={}, InProgress={}, Completed={}, Failed={}",
            worker_id, remaining, in_progress, completed, failed
        );
    }
}

// Helper function to ensure OpenSSH key is properly formatted
fn ensure_openssh_format(key_content: &str) -> Result<String> {
    // Parse the key to validate it
    let private_key = PrivateKey::from_openssh(key_content)
        .context("Failed to parse OpenSSH private key")?;
    
    // Re-encode it to ensure proper formatting
    let openssh_string = private_key
        .to_openssh(LineEnding::LF)
        .context("Failed to re-encode key to OpenSSH format")?;
    
    Ok(openssh_string.to_string())
}

// Entire SFTP session creation in blocking sync function
fn create_sftp_session_blocking(cfg: &Config) -> Result<ssh2::Session> {
    // Connect to the SFTP server
    let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))
        .context("Failed to connect to SFTP server")?;
    let mut sess = Session::new().context("Failed to create SSH session")?;
    sess.set_tcp_stream(tcp);
    sess.handshake().context("SSH handshake failed")?;

    // Authenticate using private key
    if let Some(ref key_content) = cfg.bank_private_key_content {
        // Ensure the key is properly formatted
        let formatted_key = ensure_openssh_format(key_content)
            .unwrap_or_else(|_| key_content.clone());
        
        // Write the key to a temporary file for authentication
        let temp_key_path = format!("/tmp/sftp_temp_key_{}.pem", process::id());
        fs::write(&temp_key_path, &formatted_key)
            .context("Failed to write private key to temporary file")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp_key_path, fs::Permissions::from_mode(0o600))?;
        }

        let auth_result = sess.userauth_pubkey_file(&cfg.bank_username, None, Path::new(&temp_key_path), None);
        
        // Clean up temp file
        let _ = fs::remove_file(&temp_key_path);
        
        auth_result.context("SSH authentication failed with key content")?;
    } else if let Some(ref path) = cfg.bank_private_key_path {
        sess.userauth_pubkey_file(&cfg.bank_username, None, Path::new(path), None)
            .context("SSH authentication failed")?;
    } else {
        return Err(anyhow::anyhow!("No private key provided"));
    }

    if !sess.authenticated() {
        return Err(anyhow::anyhow!("SSH authentication failed"));
    }

    println!("✓ SSH authenticated for {}@{}:{}", cfg.bank_username, cfg.bank_host, cfg.bank_port);

    Ok(sess)
}

// Blocking recursive listing using an SSH session (blocking)
fn list_sftp_files_recursively(sess: &Session, path: &Path) -> Result<Vec<String>> {
    let sftp = sess.sftp().context("Failed to open SFTP session for listing")?;
    let mut files = Vec::new();

    // Try reading directory
    let entries = match sftp.readdir(path) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("⚠️ Cannot read {:?}: {}", path, err);
            return Ok(files);
        }
    };

    for (entry_path, stat) in entries {
        let file_name = entry_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        if file_name.starts_with('.') {
            continue; // skip hidden files
        }

        let full_path = entry_path.to_string_lossy().to_string();

        if stat.is_dir() {
            let nested = list_sftp_files_recursively(sess, &entry_path)?;
            files.extend(nested);
        } else {
            files.push(full_path);
        }
    }

    Ok(files)
}

// Blocking download of a single file using provided session.
// returns bytes written
fn blocking_download_file(sess: &Session, cfg: &Config, remote_file_path: &str) -> Result<u64> {
    let sftp = sess.sftp().context("Failed to open SFTP subsystem for download")?;

    // Determine the local file path
    let remote_path_trimmed = remote_file_path.trim_start_matches('/');
    let local_file_path = Path::new(&cfg.local_storage_path).join(remote_path_trimmed);

    println!(
        "[Blocking] Downloading {} -> {}",
        remote_file_path,
        local_file_path.display()
    );

    // Create parent directories if they don't exist
    if let Some(parent) = local_file_path.parent() {
        fs::create_dir_all(parent)
            .context(format!("Failed to create directory: {}", parent.display()))?;
    }

    // Create temporary file for atomic write
    let temp_file_path = local_file_path.with_extension("tmp");

    // Open SFTP file for reading
    let mut remote_file = sftp
        .open(Path::new(remote_file_path))
        .context(format!("SFTP file open failed: {}", remote_file_path))?;

    // Open local file for writing
    let mut local_file = fs::File::create(&temp_file_path)
        .context(format!("Failed to create local file: {}", temp_file_path.display()))?;

    let mut total_bytes: u64 = 0;
    let mut buffer = vec![0u8; cfg.upload_chunk_size];

    loop {
        match remote_file.read(&mut buffer) {
            Ok(0) => {
                // EOF
                break;
            }
            Ok(n) => {
                local_file
                    .write_all(&buffer[..n])
                    .context("Failed to write chunk to local file")?;
                total_bytes += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                continue;
            }
            Err(e) => {
                return Err(e).context("Error reading from remote SFTP file");
            }
        }
    }

    // flush & sync
    local_file.flush().context("Failed to flush local file")?;
    local_file.sync_all().context("Failed to sync local file")?;
    drop(local_file);

    // Atomic rename
    fs::rename(&temp_file_path, &local_file_path)
        .context(format!("Failed to rename temp file to final file: {}", local_file_path.display()))?;

    println!(
        "[Blocking] ✓ Download complete: {} bytes written to {}",
        total_bytes,
        local_file_path.display()
    );

    Ok(total_bytes)
}

// Monitoring task: prints metrics every MONITOR_INTERVAL_SECS
async fn monitor_task(state: Arc<FileProcessingState>, metrics: Arc<Metrics>, _cfg: Config) {
    let mut sys = System::new_all();

    let start = Instant::now();

    loop {
        sleep(Duration::from_secs(MONITOR_INTERVAL_SECS)).await;
        sys.refresh_all();

        let cpu_usage = sys.global_cpu_usage(); // percentage 0..100

        let total_mem_kb = sys.total_memory();
        let used_mem_kb = sys.used_memory();

        let (total_bytes, files_completed, files_failed, total_time_ns) = metrics.snapshot();
        let elapsed = start.elapsed();

        let (remaining, in_progress, completed_set_len, failed_set_len) = state.get_stats().await;

        // compute instantaneous throughput over the last interval using metrics.total_bytes is coarse,
        // but we will print total and avg speed
        let total_mb = total_bytes as f64 / (1024.0 * 1024.0);
        let elapsed_s = elapsed.as_secs_f64();
        let avg_mb_s = if elapsed_s > 0.0 { total_mb / elapsed_s } else { 0.0 };

        println!("\n--- Monitor Snapshot ---");
        println!("Elapsed: {:.2}s", elapsed_s);
        println!("CPU Usage: {:.2}%", cpu_usage);
        println!("Memory: used {} KB / total {} KB", used_mem_kb, total_mem_kb);
        println!("Total data transferred (metric): {:.2} MB", total_mb);
        println!("Avg throughput: {:.3} MB/s", avg_mb_s);
        println!("Files completed (metric counter): {}", files_completed);
        println!("Files failed (metric counter): {}", files_failed);
        println!("Total file-processing time (s): {:.3}", (total_time_ns as f64) / 1_000_000_000.0);
        println!(
            "Queue: Remaining={}, InProgress={}, Completed_set_len={}, Failed_set_len={}",
            remaining, in_progress, completed_set_len, failed_set_len
        );
        println!("--- End Snapshot ---\n");
    }
}
