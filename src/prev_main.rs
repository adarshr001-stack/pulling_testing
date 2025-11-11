use std::net::TcpStream;
use anyhow::{Result, Context};
use std::env;
use std::io::{Read, Write};
use ssh2::{Session, File};
use std::path::Path;
use std::fs;
use tokio;
use std::sync::Arc;
//env UPLOAD_CHUNK_SIZE_MB=10 time -l ./target/release/sftp_puller_main

use std::process;

// --- Multi-threading Imports ---
use tokio::sync::{Semaphore, Mutex};
use std::collections::{HashSet, VecDeque};

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
        dotenv::dotenv().ok();

        Ok(Self {
            bank_host: env::var("BANK_HOST")?,
            bank_port: env::var("BANK_PORT")?.parse()?,
            bank_username: env::var("BANK_USERNAME")?,
            bank_private_key_path: env::var("BANK_PRIVATE_KEY_PATH").ok(),
            bank_private_key_content: env::var("BANK_PRIVATE_KEY_CONTENT").ok(),
            bank_remote_path: env::var("BANK_REMOTE_PATH")?,
            local_storage_path: env::var("LOCAL_STORAGE_PATH")?,
            upload_chunk_size: env::var("UPLOAD_CHUNK_SIZE_MB")
                .unwrap_or_else(|_| "10".to_string())
                .parse::<usize>()? * 1024 * 1024,
            max_concurrent_uploads: env::var("MAX_CONCURRENT_UPLOADS")
                .unwrap_or_else(|_| "4".to_string())
                .parse::<usize>()?,
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

#[tokio::main]
async fn main() -> Result<()> {
    println!("PROCESS ID: {}", process::id());
    let cfg = Config::from_env()?;

    println!("Loaded config: {:?}", cfg);
    println!("Max concurrent uploads: {}", cfg.max_concurrent_uploads);

    // Connect to the bank sftp server to list files (initial connection)
    let initial_sftp = create_sftp_session(&cfg).await?;
    
    // List all files recursively
    let all_files = list_sftp_files_recursively(&initial_sftp, Path::new(&cfg.bank_remote_path))?;
    println!("Found {} files on SFTP server", all_files.len());
    
    if all_files.is_empty() {
        println!("No files to process.");
        return Ok(());
    }

    // Create shared state for tracking file processing
    let state = Arc::new(FileProcessingState::new(all_files));
    
    // Create semaphore to limit concurrent uploads
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrent_uploads));
    
    // Ensure local storage directory exists
    fs::create_dir_all(&cfg.local_storage_path)
        .context("Failed to create local storage directory")?;
    println!("Local storage directory: {}", cfg.local_storage_path);
    
    // Spawn worker tasks
    let mut handles = vec![];
    
    for worker_id in 0..cfg.max_concurrent_uploads {
        let cfg_clone = cfg.clone();
        let state_clone = Arc::clone(&state);
        let semaphore_clone = Arc::clone(&semaphore);
        
        let handle = tokio::spawn(async move {
            worker_task(
                worker_id,
                cfg_clone,
                state_clone,
                semaphore_clone,
            ).await
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
    let (remaining, in_progress, completed, failed) = state.get_stats().await;
    println!("\n=== Final Statistics ===");
    println!("Completed: {}", completed);
    println!("Failed: {}", failed);
    println!("In Progress: {}", in_progress);
    println!("Remaining: {}", remaining);
    
    if failed > 0 {
        println!("\n⚠️ Some files failed to upload. Check logs above for details.");
    } else {
        println!("\n✅ All files transferred successfully!");
    }
    
    Ok(())
}

// Worker task that processes files from the queue
async fn worker_task(
    worker_id: usize,
    cfg: Config,
    state: Arc<FileProcessingState>,
    semaphore: Arc<Semaphore>,
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
        
        // Acquire semaphore permit (limits concurrent connections)
        let _permit = semaphore.acquire().await.unwrap();
        
        println!("[Worker {}] Processing file: {}", worker_id, file_path);
        
        // Create a new SFTP session for this worker
        // (SFTP sessions are not thread-safe, each worker needs its own)
        let sftp_result = create_sftp_session(&cfg).await;
        
        match sftp_result {
            Ok(sftp) => {
                // Process the file
                let mut cfg_file = cfg.clone();
                cfg_file.bank_remote_path = file_path.clone();
                
                match download_to_local_disk(worker_id, &cfg_file, Arc::new(sftp)).await {
                    Ok(_) => {
                        println!("[Worker {}] ✅ Successfully downloaded: {}", worker_id, file_path);
                        state.mark_completed(&file_path).await;
                    }
                    Err(e) => {
                        eprintln!("[Worker {}] ❌ Failed to download {}: {}", worker_id, file_path, e);
                        state.mark_failed(&file_path).await;
                    }
                }
            }
            Err(e) => {
                eprintln!("[Worker {}] ❌ Failed to create SFTP session for {}: {}", worker_id, file_path, e);
                state.mark_failed(&file_path).await;
            }
        }
        
        // Print current statistics
        let (remaining, in_progress, completed, failed) = state.get_stats().await;
        println!("[Worker {}] Progress: Remaining={}, InProgress={}, Completed={}, Failed={}", 
                 worker_id, remaining, in_progress, completed, failed);
    }
}

use ssh2::Sftp;

fn list_sftp_files_recursively(sftp: &Sftp, path: &Path) -> Result<Vec<String>> {
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
        let file_name = entry_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        if file_name.starts_with('.') {
            continue; // skip hidden files
        }

        // Construct full path as string
        let full_path = entry_path.to_string_lossy().to_string();

        if stat.is_dir() {
            // Recursive call for subfolder
            let mut nested = list_sftp_files_recursively(sftp, &entry_path)?;
            files.append(&mut nested);
        } else {
            // Add file path
            files.push(full_path);
        }
    }

    Ok(files)
}

async fn create_sftp_session(cfg: &Config) -> Result<ssh2::Sftp> {
    // Connect to the SFTP server
    let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))
        .context("Failed to connect to SFTP server")?;
    let mut sess = Session::new().context("Failed to create SSH session")?;
    sess.set_tcp_stream(tcp);
    sess.handshake().context("SSH handshake failed")?;

    // Authenticate using private key
    if let Some(ref key_content) = cfg.bank_private_key_content {
        // Write the PEM key directly to temp file
        let temp_key_path = format!("/tmp/sftp_temp_key_{}.pem", process::id());
        
        // Format the key properly - replace spaces with newlines in the base64 content
        let formatted_key = if key_content.contains("-----BEGIN") && key_content.contains("-----END") {
            // Extract header, content, and footer
            let parts: Vec<&str> = key_content.split("-----").collect();
            if parts.len() >= 5 {
                let header = format!("-----{}-----", parts[1]);
                let footer = format!("-----{}-----", parts[3]);
                let content = parts[2].trim();
                
                // Split content into 64-character lines (standard PEM format)
                let mut formatted_content = String::new();
                for chunk in content.split_whitespace() {
                    formatted_content.push_str(chunk);
                }
                
                let mut lines = Vec::new();
                let chars: Vec<char> = formatted_content.chars().collect();
                for chunk in chars.chunks(64) {
                    lines.push(chunk.iter().collect::<String>());
                }
                
                format!("{}\n{}\n{}", header, lines.join("\n"), footer)
            } else {
                key_content.clone()
            }
        } else {
            key_content.clone()
        };
        
        fs::write(&temp_key_path, formatted_key)
            .context("Failed to write private key to temporary file")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp_key_path, fs::Permissions::from_mode(0o600))?;
        }

        // Authenticate using the temporary key file
        sess.userauth_pubkey_file(
            &cfg.bank_username,
            None,
            Path::new(&temp_key_path),
            None,
        ).context("SSH authentication failed")?;

        // Clean up the temporary file
        let _ = fs::remove_file(&temp_key_path);
    } else if let Some(ref path) = cfg.bank_private_key_path {
        sess.userauth_pubkey_file(&cfg.bank_username, None, Path::new(path), None)
            .context("SSH authentication failed")?;
    } else {
        return Err(anyhow::anyhow!("No private key provided"));
    }

    if !sess.authenticated() {
        return Err(anyhow::anyhow!("SSH authentication failed"));
    }

    // Open SFTP session
    let sftp = sess.sftp().context("Failed to open SFTP session")?;
        println!("✓ SFTP session established!");

    Ok(sftp)
}

async fn download_to_local_disk(
    worker_id: usize,
    cfg: &Config,
    sftp: Arc<ssh2::Sftp>,
) -> Result<()> {
    // Determine the local file path
    // Remove leading slash from remote path to create relative path
    let remote_path = cfg.bank_remote_path.trim_start_matches('/');
    let local_file_path = Path::new(&cfg.local_storage_path).join(remote_path);
    
    println!("[Worker {}] Downloading {} -> {}", worker_id, cfg.bank_remote_path, local_file_path.display());
    
    // Create parent directories if they don't exist
    if let Some(parent) = local_file_path.parent() {
        fs::create_dir_all(parent)
            .context(format!("Failed to create directory: {}", parent.display()))?;
    }
    
    // Create temporary file for atomic write
    let temp_file_path = local_file_path.with_extension("tmp");
    
    let transfer_result = async {
        // Open SFTP file for reading
        let mut sftp_file = tokio::task::spawn_blocking({
            let sftp_path = cfg.bank_remote_path.clone();
            let sftp_arc = Arc::clone(&sftp);
            move || -> Result<File> {
                sftp_arc.open(Path::new(&sftp_path))
                    .context(format!("SFTP file open failed: {}", sftp_path))
            }
        }).await??;

        // Open local file for writing
        let mut local_file = fs::File::create(&temp_file_path)
            .context(format!("Failed to create local file: {}", temp_file_path.display()))?;

        let mut total_bytes = 0;
        let mut chunk_number = 1;

        // Stream data in chunks
        loop {
            let (file_out, read_result) = tokio::task::spawn_blocking({
                let mut sftp_file_ref = sftp_file;
                let mut chunk_buffer = vec![0u8; cfg.upload_chunk_size];
                let mut bytes_in_buffer = 0;

                move || -> (File, std::io::Result<(Vec<u8>, usize)>) {
                    loop {
                        let bytes_read = match sftp_file_ref.read(&mut chunk_buffer[bytes_in_buffer..]) {
                            Ok(0) => break,
                            Ok(n) => n,
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            Err(e) => return (sftp_file_ref, Err(e)),
                        };

                        bytes_in_buffer += bytes_read;

                        if bytes_in_buffer == chunk_buffer.len() {
                            break;
                        }
                    }

                    (sftp_file_ref, Ok((chunk_buffer, bytes_in_buffer)))
                }
            }).await?;

            sftp_file = file_out;
            let (buffer, bytes_read) = read_result?;

            if bytes_read == 0 {
                println!("[Worker {}] ({}) End of file reached. Total bytes: {}", 
                         worker_id, cfg.bank_remote_path, total_bytes);
                break;
            }

            // Write chunk to local file
            local_file.write_all(&buffer[..bytes_read])
                .context(format!("Failed to write chunk {} to local file", chunk_number))?;
            
            total_bytes += bytes_read;
            println!("[Worker {}] ({}) Written chunk {} ({} bytes, total: {} bytes)", 
                     worker_id, cfg.bank_remote_path, chunk_number, bytes_read, total_bytes);
            
            chunk_number += 1;
        }

        // Flush and sync to ensure all data is written
        local_file.flush()
            .context("Failed to flush local file")?;
        local_file.sync_all()
            .context("Failed to sync local file")?;
        
        drop(local_file);

        // Atomic rename from temp file to final file
        fs::rename(&temp_file_path, &local_file_path)
            .context(format!("Failed to rename temp file to final file: {}", local_file_path.display()))?;

        println!("[Worker {}] ({}) ✓ Download complete: {} bytes written to {}", 
                 worker_id, cfg.bank_remote_path, total_bytes, local_file_path.display());

        Ok(())
    }.await;

    // Clean up temp file on error
    if let Err(e) = &transfer_result {
        eprintln!("[Worker {}] ({}) Error during download: {}", worker_id, cfg.bank_remote_path, e);
        if temp_file_path.exists() {
            if let Err(remove_err) = fs::remove_file(&temp_file_path) {
                eprintln!("[Worker {}] ({}) Failed to remove temp file: {}", worker_id, cfg.bank_remote_path, remove_err);
            }
        }
    }

    transfer_result
}
