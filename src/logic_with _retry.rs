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
}

impl SftpConnection {
    async fn new(config: Config) -> Result<Self> {
        let sftp = create_sftp_session_with_retry(&config).await?;
        Ok(Self {
            config,
            sftp: Arc::new(Mutex::new(Some(sftp))),
        })
    }

    async fn reconnect(&self) -> Result<()> {
        println!("🔄 Attempting to reconnect SFTP session...");
        let new_sftp = create_sftp_session_with_retry(&self.config).await?;
        let mut sftp_guard = self.sftp.lock().await;
        *sftp_guard = Some(new_sftp);
        println!("✅ SFTP session reconnected successfully");
        Ok(())
    }

    async fn get_sftp(&self) -> Arc<Mutex<Option<ssh2::Sftp>>> {
        Arc::clone(&self.sftp)
    }
}

#[tokio::main]
async fn main() -> Result<()> {   
    println!("PROCESS ID: {}", process::id());
    let cfg = Config::from_env()?;

    println!("Loaded config: {:?}", cfg);

    // Create SFTP connection with retry support
    let sftp_conn = SftpConnection::new(cfg.clone()).await?;
    let s3_client = create_s3_client(&cfg).await?;

    // List all files recursively
    let all_files = {
        let sftp_guard = sftp_conn.get_sftp().await;
        let sftp_lock = sftp_guard.lock().await;
        let sftp_ref = sftp_lock.as_ref().ok_or_else(|| anyhow!("SFTP connection not available"))?;
        list_sftp_files_recursively(sftp_ref, Path::new(&cfg.bank_remote_path))?
    };
    
    println!("Found {} files on SFTP server", all_files.len());

    // Process each file sequentially
    for remote_path in all_files {
        println!("\n=== Processing file: {:?} ===", remote_path);
        
        let mut cfg_file = cfg.clone();
        cfg_file.bank_remote_path = remote_path.clone();
        
        if let Err(e) = upload_to_storage_with_retry(&cfg_file, &sftp_conn, s3_client.clone()).await {
            eprintln!("❌ Failed to upload {:?}: {}", remote_path, e);
        } else {
            println!("✅ Uploaded {:?}", remote_path);
        }
    }

    println!("Successfully transferred all files.");
    Ok(())
}

use ssh2::Sftp;

fn list_sftp_files_recursively(sftp: &Sftp, path: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();

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
            continue;
        }

        let full_path = entry_path.to_string_lossy().to_string();

        if stat.is_dir() {
            let mut nested = list_sftp_files_recursively(sftp, &entry_path)?;
            files.append(&mut nested);
        } else {
            files.push(full_path);
        }
    }

    Ok(files)
}

// Create SFTP session with retry logic
async fn create_sftp_session_with_retry(cfg: &Config) -> Result<ssh2::Sftp> {
    let mut attempts = 0;
    let max_attempts = cfg.max_retries + 1;

    loop {
        attempts += 1;
        
        match create_sftp_session(cfg).await {
            Ok(sftp) => {
                if attempts > 1 {
                    println!("✅ Successfully connected after {} attempts", attempts);
                }
                return Ok(sftp);
            }
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(anyhow!("Failed to connect after {} attempts: {}", attempts, e));
                }
                
                eprintln!("⚠️ Connection attempt {}/{} failed: {}", attempts, max_attempts, e);
                eprintln!("🔄 Retrying in {} seconds...", cfg.retry_delay_secs);
                
                tokio::time::sleep(Duration::from_secs(cfg.retry_delay_secs)).await;
            }
        }
    }
}

async fn create_sftp_session(cfg: &Config) -> Result<ssh2::Sftp> {
    let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))
        .context("Failed to connect to SFTP server")?;
    
    let mut sess = Session::new().context("Failed to create SSH session")?;
    sess.set_tcp_stream(tcp);
    sess.handshake().context("SSH handshake failed")?;

    if let Some(ref key_content) = cfg.bank_private_key_content {
        let temp_key_path = "/tmp/sftp_temp_key.pem";
        
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
        
        fs::write(temp_key_path, formatted_key)
            .context("Failed to write private key to temporary file")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(temp_key_path, fs::Permissions::from_mode(0o600))?;
        }

        println!("✓ Using private key from BANK_PRIVATE_KEY_CONTENT");

        sess.userauth_pubkey_file(
            &cfg.bank_username,
            None,
            Path::new(temp_key_path),
            None,
        ).context("SSH authentication failed")?;

        let _ = fs::remove_file(temp_key_path);
    } else if let Some(ref path) = cfg.bank_private_key_path {
        println!("✓ Using private key from path: {}", path);
        sess.userauth_pubkey_file(&cfg.bank_username, None, Path::new(path), None)
            .context("SSH authentication failed")?;
    } else {
        return Err(anyhow::anyhow!("No private key provided"));
    }

    if !sess.authenticated() {
        return Err(anyhow::anyhow!("SSH authentication failed"));
    }

    let sftp = sess.sftp().context("Failed to open SFTP session")?;
    println!("✓ SFTP session established!");
    Ok(sftp)
}

async fn create_s3_client(cfg: &Config) -> Result<S3Client> {
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

// Wrapper function with retry logic
async fn upload_to_storage_with_retry(
    cfg: &Config,
    sftp_conn: &SftpConnection,
    s3_client: S3Client,
) -> Result<()> {
    let mut retry_count = 0;
    let max_retries = cfg.max_retries;

    loop {
        match upload_to_storage(cfg, sftp_conn, s3_client.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let error_msg = format!("{}", e);
                
                // Check if it's a connection error that we should retry
                let should_retry = error_msg.contains("connection")
                    || error_msg.contains("Connection")
                    || error_msg.contains("broken pipe")
                    || error_msg.contains("timeout")
                    || error_msg.contains("EOF");

                if should_retry && retry_count < max_retries {
                    retry_count += 1;
                    eprintln!("⚠️ Upload failed (attempt {}/{}): {}", retry_count, max_retries + 1, e);
                    eprintln!("🔄 Reconnecting and retrying in {} seconds...", cfg.retry_delay_secs);
                    
                    tokio::time::sleep(Duration::from_secs(cfg.retry_delay_secs)).await;
                    
                    // Attempt to reconnect
                    if let Err(reconnect_err) = sftp_conn.reconnect().await {
                        eprintln!("⚠️ Reconnection failed: {}", reconnect_err);
                        continue;
                    }
                } else {
                    return Err(e);
                }
            }
        }
    }
}

async fn upload_to_storage(
    cfg: &Config,
    sftp_conn: &SftpConnection,
    s3_client: S3Client,
) -> Result<()> {
    
    // 1. Initiate Multipart Upload
    let mpu = s3_client
        .create_multipart_upload()
        .bucket(cfg.storage_bucket.clone())
        .key(cfg.bank_remote_path.clone())
        .send()
        .await?;

    let upload_id = mpu.upload_id().ok_or_else(|| anyhow!("S3 response missing UploadId"))?;
    println!("📤 Initiated MPU. UploadId: {}", upload_id);

    let transfer_result = async { 
        let mut part_number = 1;
        let mut completed_parts = Vec::new();
        let mut total_bytes_read: u64 = 0;

        // Open SFTP file with retry logic
        let mut sftp_file = {
            let sftp_guard = sftp_conn.get_sftp().await;
            let sftp_lock = sftp_guard.lock().await;
            let sftp_ref = sftp_lock.as_ref().ok_or_else(|| anyhow!("SFTP connection not available"))?;
            
            tokio::task::spawn_blocking({
                let sftp_path = cfg.bank_remote_path.clone();
                let sftp_clone = unsafe { std::mem::transmute::<&ssh2::Sftp, &'static ssh2::Sftp>(sftp_ref) };
                
                move || -> Result<File> {
                    sftp_clone.open(Path::new(&sftp_path))
                        .context(format!("SFTP file open failed: {}", sftp_path))
                }
            }).await??
        };

        // Stream parts with connection recovery
        loop {
            let (file_out, read_result) = tokio::task::spawn_blocking({
                let mut sftp_file_ref = sftp_file;
                let chunk_size = cfg.upload_chunk_size;
                let current_offset = total_bytes_read;
                
                move || -> (File, std::io::Result<(Vec<u8>, usize)>) {
                    let mut chunk_buffer = vec![0u8; chunk_size];
                    let mut bytes_in_buffer = 0;
                    let mut retry_on_this_read = 0;
                    let max_read_retries = 3;

                    loop {
                        let bytes_read = match sftp_file_ref.read(&mut chunk_buffer[bytes_in_buffer..]) {
                            Ok(0) => {
                                break; // EOF
                            },
                            Ok(n) => n,
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            Err(e) if is_connection_error(&e) && retry_on_this_read < max_read_retries => {
                                retry_on_this_read += 1;
                                eprintln!("⚠️ Read error (retry {}/{}): {}", retry_on_this_read, max_read_retries, e);
                                thread::sleep(Duration::from_secs(2));
                                continue;
                            },
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
            
            let (mut buffer, bytes_read) = match read_result {
                Ok(result) => result,
                Err(e) if is_connection_error(&e) => {
                    eprintln!("⚠️ Connection error detected during read: {}", e);
                    return Err(anyhow!("SFTP connection error: {}", e));
                }
                Err(e) => return Err(anyhow!("SFTP read error: {}", e)),
            };

            if bytes_read == 0 {
                println!("📄 End of file reached. Total bytes read: {}", total_bytes_read);
                break;
            }

            total_bytes_read += bytes_read as u64;
            println!("📊 Read chunk of {} bytes for part {} (total: {} bytes)", 
                bytes_read, part_number, total_bytes_read);

            buffer.truncate(bytes_read);

            // Upload part with retry
            let mut upload_retry = 0;
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
                    Err(e) if upload_retry < 3 => {
                        upload_retry += 1;
                        eprintln!("⚠️ S3 upload part {} retry {}/3: {}", part_number, upload_retry, e);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    Err(e) => return Err(anyhow!("S3 UploadPart {} failed: {}", part_number, e)),
                }
            };
            
            let etag = part_resp.e_tag()
                .ok_or_else(|| anyhow!("S3 response missing ETag for part {}", part_number))?;
            
            println!("✅ Uploaded part {} (ETag: {})", part_number, etag);

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
            println!("🏁 Completing multipart upload...");
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
            
            println!("✅ Multipart upload completed successfully");
        } else {
            println!("📄 File was empty, handling as zero-byte file");
            
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

    if let Err(e) = &transfer_result {
        eprintln!("❌ Transfer failed: {}", e);
        eprintln!("🧹 Aborting multipart upload...");
        
        if let Err(abort_err) = s3_client
            .abort_multipart_upload()
            .bucket(cfg.storage_bucket.clone())
            .key(cfg.bank_remote_path.clone())
            .upload_id(upload_id)
            .send()
            .await 
        {
            eprintln!("⚠️ Failed to abort MPU: {}", abort_err);
        }
    }
    
    transfer_result
}
