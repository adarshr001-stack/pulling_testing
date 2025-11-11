use std::net::TcpStream;
use anyhow::{Result, Context, anyhow};
use std::env;
use std::io::{Read, Write, Seek, SeekFrom};
use ssh2::{Session, File};
use std::path::Path;
use std::fs;

// --- AWS Imports ---
use aws_config::{self};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Builder, primitives::ByteStream, Client as S3Client,
    types::{CompletedMultipartUpload, CompletedPart},
};
use aws_types::region::Region;

// --- Tokio/Bytes Imports ---
use bytes::Bytes;
use tokio;
use std::sync::Arc;
use tokio::sync::Mutex;

use std::process;
use std::thread;
use std::time::Duration;


#[derive(Debug, Clone)]
struct Config {
    bank_host: String,
    bank_port: u16,
    bank_username: String,
    bank_private_key_path: Option<String>,
    bank_private_key_content: Option<String>,
    bank_remote_path: String,

    storage_endpoint: String,
    storage_access_key: String,
    storage_secret_key: String,
    storage_bucket: String,
    storage_region: String,

    upload_chunk_size: usize,
    
    // Connection retry settings
    max_retries: u32,
    retry_delay_secs: u64,
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
            storage_endpoint: env::var("STORAGE_ENDPOINT")?,
            storage_access_key: env::var("STORAGE_ACCESS_KEY")?,
            storage_secret_key: env::var("STORAGE_SECRET_KEY")?,
            storage_bucket: env::var("STORAGE_BUCKET")?,
            storage_region: env::var("STORAGE_REGION")?,
            upload_chunk_size: env::var("UPLOAD_CHUNK_SIZE_MB")
                .unwrap_or_else(|_| "10".to_string())
                .parse::<usize>()? * 1024 * 1024,
            max_retries: env::var("SFTP_MAX_RETRIES")
                .unwrap_or_else(|_| "3".to_string())
                .parse()?,
            retry_delay_secs: env::var("SFTP_RETRY_DELAY_SECS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()?,
        })
    }
}

// Wrapper struct to manage SFTP connection with reconnection capability
struct SftpConnection {
    config: Config,
    sftp: Arc<Mutex<Option<ssh2::Sftp>>>,
    worker_id: usize,
}

impl SftpConnection {
    async fn new(config: Config, worker_id: usize) -> Result<Self> {
        let sftp = create_sftp_session_with_retry(&config, worker_id).await?;
        Ok(Self {
            config,
            sftp: Arc::new(Mutex::new(Some(sftp))),
            worker_id,
        })
    }

    async fn reconnect(&self) -> Result<()> {
        println!("[Worker {}] 🔄 Attempting to reconnect SFTP session...", self.worker_id);
        let new_sftp = create_sftp_session_with_retry(&self.config, self.worker_id).await?;
        let mut sftp_guard = self.sftp.lock().await;
        *sftp_guard = Some(new_sftp);
        println!("[Worker {}] ✅ SFTP session reconnected successfully", self.worker_id);
        Ok(())
    }

    async fn get_sftp(&self) -> Arc<Mutex<Option<ssh2::Sftp>>> {
        Arc::clone(&self.sftp)
    }
}

#[tokio::main]
async fn main() -> Result<()> {   
    let worker_id = 1; // Main worker ID (for single-threaded mode)
    
    println!("[Worker {}] PROCESS ID: {}", worker_id, process::id());
    let cfg = Config::from_env()?;

    println!("[Worker {}] Loaded config: {:?}", worker_id, cfg);

    // Create SFTP connection with retry support
    let sftp_conn = SftpConnection::new(cfg.clone(), worker_id).await?;
    let s3_client = create_s3_client(&cfg, worker_id).await?;

    // List all files recursively
    let all_files = {
        let sftp_guard = sftp_conn.get_sftp().await;
        let sftp_lock = sftp_guard.lock().await;
        let sftp_ref = sftp_lock.as_ref().ok_or_else(|| anyhow!("SFTP connection not available"))?;
        list_sftp_files_recursively(sftp_ref, Path::new(&cfg.bank_remote_path), worker_id)?
    };
    
    println!("[Worker {}] Found {} files on SFTP server", worker_id, all_files.len());

    // Process each file sequentially
    for remote_path in all_files {
        println!("[Worker {}]\n=== Processing file: {:?} ===", worker_id, remote_path);
        
        let mut cfg_file = cfg.clone();
        cfg_file.bank_remote_path = remote_path.clone();
        
        if let Err(e) = upload_to_storage_with_retry(&cfg_file, &sftp_conn, s3_client.clone()).await {
            eprintln!("[Worker {}] ❌ Failed to upload {:?}: {}", worker_id, remote_path, e);
        } else {
            println!("[Worker {}] ✅ Uploaded {:?}", worker_id, remote_path);
        }
    }

    println!("[Worker {}] Successfully transferred all files.", worker_id);
    Ok(())
}

use ssh2::Sftp;

fn list_sftp_files_recursively(sftp: &Sftp, path: &Path, worker_id: usize) -> Result<Vec<String>> {
    let mut files = Vec::new();

    let entries = match sftp.readdir(path) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("[Worker {}] ⚠️ Cannot read {:?}: {}", worker_id, path, err);
            return Ok(files);
        }
    };

    for (entry_path, stat) in entries {
        let file_name = entry_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        if file_name.starts_with('.') {
            continue;
        }

        let full_path = entry_path.to_string_lossy().to_string();

        if stat.is_dir() {
            let mut nested = list_sftp_files_recursively(sftp, &entry_path, worker_id)?;
            files.append(&mut nested);
        } else {
            files.push(full_path);
        }
    }

    Ok(files)
}

// Create SFTP session with retry logic
async fn create_sftp_session_with_retry(cfg: &Config, worker_id: usize) -> Result<ssh2::Sftp> {
    let mut attempts = 0;
    let max_attempts = cfg.max_retries + 1;

    loop {
        attempts += 1;
        
        match create_sftp_session(cfg, worker_id).await {
            Ok(sftp) => {
                if attempts > 1 {
                    println!("[Worker {}] ✅ Successfully connected after {} attempts", worker_id, attempts);
                }
                return Ok(sftp);
            }
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(anyhow!("Failed to connect after {} attempts: {}", attempts, e));
                }
                
                eprintln!("[Worker {}] ⚠️ Connection attempt {}/{} failed: {}", worker_id, attempts, max_attempts, e);
                eprintln!("[Worker {}] 🔄 Retrying in {} seconds...", worker_id, cfg.retry_delay_secs);
                
                tokio::time::sleep(Duration::from_secs(cfg.retry_delay_secs)).await;
            }
        }
    }
}

async fn create_sftp_session(cfg: &Config, worker_id: usize) -> Result<ssh2::Sftp> {
    let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))
        .context("Failed to connect to SFTP server")?;
    
    let mut sess = Session::new().context("Failed to create SSH session")?;
    sess.set_tcp_stream(tcp);
    sess.handshake().context("SSH handshake failed")?;

    if let Some(ref key_content) = cfg.bank_private_key_content {
        let temp_key_path = format!("/tmp/sftp_temp_key_worker_{}.pem", worker_id);
        
        let formatted_key = if key_content.contains("-----BEGIN") && key_content.contains("-----END") {
            let parts: Vec<&str> = key_content.split("-----").collect();
            if parts.len() >= 5 {
                let header = format!("-----{}-----", parts[1]);
                let footer = format!("-----{}-----", parts[3]);
                let content = parts[2].trim();
                
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

        println!("[Worker {}] ✓ Using private key from BANK_PRIVATE_KEY_CONTENT", worker_id);

        sess.userauth_pubkey_file(
            &cfg.bank_username,
            None,
            Path::new(&temp_key_path),
            None,
        ).context("SSH authentication failed")?;

        let _ = fs::remove_file(&temp_key_path);
    } else if let Some(ref path) = cfg.bank_private_key_path {
        println!("[Worker {}] ✓ Using private key from path: {}", worker_id, path);
        sess.userauth_pubkey_file(&cfg.bank_username, None, Path::new(path), None)
            .context("SSH authentication failed")?;
    } else {
        return Err(anyhow::anyhow!("No private key provided"));
    }

    if !sess.authenticated() {
        return Err(anyhow::anyhow!("SSH authentication failed"));
    }

    let sftp = sess.sftp().context("Failed to open SFTP session")?;
    println!("[Worker {}] ✓ SFTP session established!", worker_id);
    Ok(sftp)
}

async fn create_s3_client(cfg: &Config, worker_id: usize) -> Result<S3Client> {
    println!("[Worker {}] Creating S3 client...", worker_id);
    
    let region = Region::new(cfg.storage_region.clone());

    let base_config = aws_config::from_env()
        .region(region.clone())
        .load()
        .await;

    let credentials = Credentials::new(
        &cfg.storage_access_key,
        &cfg.storage_secret_key,
        None,
        None,
        "custom",
    );

    let credentials_provider = SharedCredentialsProvider::new(credentials);

    let s3_config = Builder::from(&base_config)
        .region(region)
        .endpoint_url(cfg.storage_endpoint.clone())
        .credentials_provider(credentials_provider)
        .build();

    let client = S3Client::from_conf(s3_config);

    Ok(client)
}

// Helper function to check if error is connection-related
fn is_connection_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::UnexpectedEof
    )
}

// Wrapper function with retry logic - retries the ENTIRE file upload on failure
async fn upload_to_storage_with_retry(
    cfg: &Config,
    sftp_conn: &SftpConnection,
    s3_client: S3Client,
) -> Result<()> {
    let max_retries = cfg.max_retries;
    let worker_id = sftp_conn.worker_id;

    for attempt in 1..=max_retries + 1 {
        println!("[Worker {}] 📁 Attempting to upload file (attempt {}/{}): {}", 
            worker_id, attempt, max_retries + 1, cfg.bank_remote_path);
        
        match upload_to_storage(cfg, sftp_conn, s3_client.clone()).await {
            Ok(()) => {
                if attempt > 1 {
                    println!("[Worker {}] ✅ File upload succeeded after {} attempts", worker_id, attempt);
                }
                return Ok(());
            }
            Err(e) => {
                let error_msg = format!("{}", e);
                
                // Check if it's an error we should retry
                let should_retry = error_msg.contains("connection")
                    || error_msg.contains("Connection")
                    || error_msg.contains("broken pipe")
                    || error_msg.contains("timeout")
                    || error_msg.contains("EOF")
                    || error_msg.contains("SFTP")
                    || error_msg.contains("SSH");

                if should_retry && attempt <= max_retries {
                    eprintln!("[Worker {}] ⚠️ Upload failed (attempt {}/{}): {}", worker_id, attempt, max_retries + 1, e);
                    eprintln!("[Worker {}] 🔄 Will retry complete file upload after reconnecting...", worker_id);
                    
                    // Wait before retry
                    tokio::time::sleep(Duration::from_secs(cfg.retry_delay_secs)).await;
                    
                    // Attempt to reconnect SFTP
                    match sftp_conn.reconnect().await {
                        Ok(()) => {
                            println!("[Worker {}] ✅ Reconnected successfully, retrying file upload from beginning...", worker_id);
                        }
                        Err(reconnect_err) => {
                            eprintln!("[Worker {}] ⚠️ Reconnection failed: {}", worker_id, reconnect_err);
                            if attempt < max_retries + 1 {
                                eprintln!("[Worker {}] 🔄 Will retry reconnection on next attempt...", worker_id);
                                continue;
                            }
                        }
                    }
                } else {
                    if attempt > max_retries {
                        return Err(anyhow!("File upload failed after {} attempts: {}", attempt, e));
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    Err(anyhow!("File upload failed after all retry attempts"))
}

async fn upload_to_storage(
    cfg: &Config,
    sftp_conn: &SftpConnection,
    s3_client: S3Client,
) -> Result<()> {
    let worker_id = sftp_conn.worker_id;
    
    // 1. Initiate Multipart Upload
    let mpu = s3_client
        .create_multipart_upload()
        .bucket(cfg.storage_bucket.clone())
        .key(cfg.bank_remote_path.clone())
        .send()
        .await
        .context("Failed to initiate multipart upload")?;

    let upload_id = mpu.upload_id().ok_or_else(|| anyhow!("S3 response missing UploadId"))?;
    println!("[Worker {}] 📤 Initiated MPU. UploadId: {}", worker_id, upload_id);

    let transfer_result = async { 
        let mut part_number = 1;
        let mut completed_parts = Vec::new();
        let mut total_bytes_read: u64 = 0;

        // Open SFTP file - this will fail if connection is lost
        let mut sftp_file = {
            let sftp_guard = sftp_conn.get_sftp().await;
            let sftp_lock = sftp_guard.lock().await;
            let sftp_ref = sftp_lock.as_ref().ok_or_else(|| anyhow!("SFTP connection not available"))?;
            
            tokio::task::spawn_blocking({
                let sftp_path = cfg.bank_remote_path.clone();
                let sftp_clone = unsafe { std::mem::transmute::<&ssh2::Sftp, &'static ssh2::Sftp>(sftp_ref) };
                
                move || -> Result<File> {
                    sftp_clone.open(Path::new(&sftp_path))
                        .context(format!("Failed to open SFTP file: {}", sftp_path))
                }
            }).await
                .context("Task spawn failed")?
                .context("SFTP file open failed")?
        };

        // Stream parts with connection recovery
        loop {
            let (file_out, read_result) = tokio::task::spawn_blocking({
                let mut sftp_file_ref = sftp_file;
                let chunk_size = cfg.upload_chunk_size;
                
                move || -> (File, std::io::Result<(Vec<u8>, usize)>) {
                    let mut chunk_buffer = vec![0u8; chunk_size];
                    let mut bytes_in_buffer = 0;

                    // Read until buffer is full or EOF
                    loop {
                        let bytes_read = match sftp_file_ref.read(&mut chunk_buffer[bytes_in_buffer..]) {
                            Ok(0) => {
                                break; // EOF
                            },
                            Ok(n) => n,
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                                // Interrupted is recoverable, just retry
                                continue;
                            },
                            Err(e) => {
                                // Any other error - propagate it
                                // Connection errors will be caught by outer retry logic
                                return (sftp_file_ref, Err(e));
                            }
                        };

                        bytes_in_buffer += bytes_read;

                        // If buffer is full, we're done with this chunk
                        if bytes_in_buffer == chunk_buffer.len() {
                            break;
                        }
                    }

                    (sftp_file_ref, Ok((chunk_buffer, bytes_in_buffer)))
                }
            }).await?;

            sftp_file = file_out;
            
            // Check for read errors and propagate connection errors
            let (mut buffer, bytes_read) = match read_result {
                Ok(result) => result,
                Err(e) if is_connection_error(&e) => {
                    // Connection error - this will trigger reconnect and full file retry
                    return Err(anyhow!("SFTP connection lost during read: {}", e));
                }
                Err(e) => {
                    // Other IO errors
                    return Err(anyhow!("SFTP read error: {}", e));
                }
            };

            if bytes_read == 0 {
                println!("[Worker {}] 📄 End of file reached. Total bytes read: {}", worker_id, total_bytes_read);
                break;
            }

            total_bytes_read += bytes_read as u64;
            println!("[Worker {}] 📊 Read chunk of {} bytes for part {} (total: {} bytes)", 
                worker_id, bytes_read, part_number, total_bytes_read);

            buffer.truncate(bytes_read);

            // Upload part with retry (for transient S3 errors)
            let mut upload_retry = 0;
            let max_part_retries = 3;
            let part_resp = loop {
                match s3_client
                    .upload_part()
                    .bucket(cfg.storage_bucket.clone())
                    .key(cfg.bank_remote_path.clone())
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(buffer.clone()))
                    .send()
                    .await
                {
                    Ok(resp) => break resp,
                    Err(e) if upload_retry < max_part_retries => {
                        upload_retry += 1;
                        eprintln!("[Worker {}] ⚠️ S3 upload part {} failed (retry {}/{}): {}", 
                            worker_id, part_number, upload_retry, max_part_retries, e);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    Err(e) => {
                        return Err(anyhow!("S3 UploadPart {} failed after {} retries: {}", 
                            part_number, max_part_retries, e));
                    }
                }
            };
            
            let etag = part_resp.e_tag()
                .ok_or_else(|| anyhow!("S3 response missing ETag for part {}", part_number))?;
            
            println!("[Worker {}] ✅ Uploaded part {} (ETag: {})", worker_id, part_number, etag);

            completed_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build(),
            );

            part_number += 1;
        }

        // Finalize transfer
        if !completed_parts.is_empty() {
            println!("[Worker {}] 🏁 Completing multipart upload...", worker_id);
            let mpu_completion = CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

            s3_client
                .complete_multipart_upload()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .upload_id(upload_id)
                .multipart_upload(mpu_completion)
                .send()
                .await
                .context("Failed to complete multipart upload")?;
            
            println!("[Worker {}] ✅ Multipart upload completed successfully", worker_id);
        } else {
            println!("[Worker {}] 📄 File was empty, handling as zero-byte file", worker_id);
            
            s3_client
                .abort_multipart_upload()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .upload_id(upload_id)
                .send()
                .await
                .context("Failed to abort MPU for empty file")?;
            
            s3_client
                .put_object()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .body(ByteStream::from(Bytes::new()))
                .send()
                .await
                .context("Failed to upload 0-byte empty file")?;
        }

        Ok(())
    }.await;

    // Handle upload result - clean up on failure
    if let Err(e) = &transfer_result {
        eprintln!("[Worker {}] ❌ Transfer failed: {}", worker_id, e);
        eprintln!("[Worker {}] 🧹 Aborting multipart upload (upload_id: {})...", worker_id, upload_id);
        
        // Always try to abort the multipart upload to clean up partial data
        if let Err(abort_err) = s3_client
            .abort_multipart_upload()
            .bucket(cfg.storage_bucket.clone())
            .key(cfg.bank_remote_path.clone())
            .upload_id(upload_id)
            .send()
            .await 
        {
            eprintln!("[Worker {}] ⚠️ Failed to abort MPU: {}", worker_id, abort_err);
        } else {
            println!("[Worker {}] ✅ Successfully aborted partial upload", worker_id);
        }
    }
    
    transfer_result
}
