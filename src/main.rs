use std::net::TcpStream;
use anyhow::{Result, Context, anyhow};
use std::env;
use std::io::{Read, Write};
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
//env UPLOAD_CHUNK_SIZE_MB=10 time -l ./target/release/sftp_puller_main

use std::process;
use std::thread;
use std::time::Duration;


#[derive(Debug, Clone)] // Added Clone and Debug
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

    upload_chunk_size: usize, // e.g., 10 * 1024 * 1024 for 10MB

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
            upload_chunk_size: env::var("UPLOAD_CHUNK_SIZE_MB").unwrap_or_else(|_| "10".to_string()).parse::<usize>()? * 1024*1024 ,

        })
    }
}
 
 
 #[tokio::main]
async fn main() -> Result<()> {   
    println!("PROCESS ID: {}", process::id()); // <-- ADD THIS
    let cfg = Config::from_env()?;

    // --- Add this line to debug your config ---
    println!("Loaded config: {:?}", cfg);
    // ---

    // Connect to the bank sftp server via SSH
    let sftp = Arc::new(create_sftp_session(&cfg).await?);
    let s3_client = create_s3_client(&cfg).await?;

    // // Download the file and upload to object storage
    // upload_to_storage(&cfg, sftp.clone(), s3_client).await?;


     // List all files recursively
    let all_files = list_sftp_files_recursively(&sftp, Path::new(&cfg.bank_remote_path))?;
    println!("Found {} files on SFTP server", all_files.len());

    // Process each file sequentially using the SAME SFTP connection
    for remote_path in all_files {
        println!("\n=== Processing file: {:?} ===", remote_path);
        
        // Reuse the existing SFTP session - no new connection needed!
        let mut cfg_file = cfg.clone();
        cfg_file.bank_remote_path = remote_path.clone();
        if let Err(e) = upload_to_storage(&cfg_file, sftp.clone(), s3_client.clone()).await {
            eprintln!("❌ Failed to upload {:?}: {}", remote_path, e);
        } else {
            println!("✅ Uploaded {:?}", remote_path);
        }
    }

    println!("Successfully transferred file.");
    Ok(())
}




use ssh2::Sftp;
use std::fs::FileType;

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
        let temp_key_path = "/tmp/sftp_temp_key.pem";
        
        // Format the key properly - replace spaces with newlines in the base64 content
        // This handles keys that may be stored with spaces instead of newlines
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
        
        fs::write(temp_key_path, formatted_key)
            .context("Failed to write private key to temporary file")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(temp_key_path, fs::Permissions::from_mode(0o600))?;
        }

        println!("✓ Using private key from BANK_PRIVATE_KEY_CONTENT");

        // Authenticate using the temporary key file
        sess.userauth_pubkey_file(
            &cfg.bank_username,
            None,                  // No public key file needed
            Path::new(temp_key_path),
            None,                  // No passphrase
        ).context("SSH authentication failed")?;

        // Optional: delete after authentication
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

    // Open SFTP session
    let sftp = sess.sftp().context("Failed to open SFTP session")?;
    println!("✓ SFTP session established!");
    Ok(sftp)
}

// async fn create_sftp_session(cfg: &Config) -> Result<ssh2::Sftp> {
//     let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))?;
//     let mut sess = Session::new()?;
//     sess.set_tcp_stream(tcp);
//     sess.handshake()?;

//     // Authenticate using SSH private key
//     if let Some(ref key_content) = cfg.bank_private_key_content {
//         // Decode base64 private key content and write to temp file
//         use base64::{Engine as _, engine::general_purpose};
        
//         let decoded_key = general_purpose::STANDARD
//             .decode(key_content)
//             .context("Failed to decode base64 private key")?;
        
//         // Create temporary file for the key
//         let temp_key_path = "/tmp/sftp_temp_key";
//         fs::write(temp_key_path, &decoded_key)
//             .context("Failed to write private key to temporary file")?;
        
//         // Set proper permissions (600) for security
//         #[cfg(unix)]
//         {
//             use std::os::unix::fs::PermissionsExt;
//             fs::set_permissions(temp_key_path, fs::Permissions::from_mode(0o600))?;
//         }
        
//         println!("✓ Using private key from BANK_PRIVATE_KEY_CONTENT");
        
//         // Use file-based authentication with the temporary file
//         sess.userauth_pubkey_file(
//             &cfg.bank_username,
//             None,  // No public key file needed
//             Path::new(temp_key_path),
//             None,  // No passphrase
//         )?;
        
//         // Clean up the temporary file
//         let _ = fs::remove_file(temp_key_path);
//     } else {
//         println!("✓ Using private key from path: {}", cfg.bank_private_key_path);
        
//         // Use file-based authentication
//         sess.userauth_pubkey_file(
//             &cfg.bank_username,
//             None,  // No public key file needed (will be derived from private key)
//             Path::new(&cfg.bank_private_key_path),
//             None,  // No passphrase for the private key
//         )?;
//     }
    
//     println!("✓ Authenticated successfully with SSH private key!");

//     if !sess.authenticated() {
//         return Err(anyhow::anyhow!("Authentication failed"));
//     }
  
//     // Open SFTP channel
//     let sftp = sess.sftp()?;
//     println!("✓ SFTP session established!");

//     Ok(sftp)
// }


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


// --- THIS IS THE UPDATED FUNCTION ---

async fn upload_to_storage(
    cfg: &Config,
    sftp: Arc<ssh2::Sftp>,
    s3_client: S3Client,
) -> Result<()> {
    
    // 1. Initiate Upload
    let mpu = s3_client
        .create_multipart_upload()
        .bucket(cfg.storage_bucket.clone())
        .key(cfg.bank_remote_path.clone())
        .send()
        .await?;


    let upload_id = mpu.upload_id().ok_or_else(|| anyhow!("S3 response missing UploadId"))?;
    println!(" Initiated MPU. UploadId: {}", upload_id);

    // This block ensures we abort the MPU on any error
    let transfer_result = async { 
        // 2. Store State (In-Memory)
        let mut part_number = 1; // This is the in-memory offset
        let mut completed_parts = Vec::new();

        // 3. Connect to SFTP (in a blocking task)
        let mut sftp_file = tokio::task::spawn_blocking({
            let sftp_path = cfg.bank_remote_path.clone();
            let sftp_arc = Arc::clone(&sftp);
            move || -> Result<File> {
                sftp_arc.open(Path::new(&sftp_path)).context(format!("SFTP file open failed: {}", sftp_path))
            }
        }).await??; // Double ?? - one for spawn_blocking, one for inner Result

        // We no longer create the buffer out here. It will be created
        // *inside* the blocking task to ensure it's filled.

       // 4. Stream Parts
        loop {
            // This blocking task now contains the "fill the buffer" logic
            let (file_out, read_result) = tokio::task::spawn_blocking({
                let mut sftp_file_ref = sftp_file; // Take ownership
                
                // --- NEW LOGIC ---
                // 1. Create the buffer *inside* the task.
                let mut chunk_buffer = vec![0u8; cfg.upload_chunk_size];

    //             // --- PAUSE IS ADDED HERE ---
    // // println!(">>> BUFFER ALLOCATED ({} bytes). Check RAM now!", cfg_clone.upload_chunk_size);
    // println!(">>> Pausing for 20 seconds...");
    // thread::sleep(Duration::from_secs(20));
    // println!(">>> Resuming...");
    // // --- END OF PAUSE ---

                let mut bytes_in_buffer = 0;

                move || -> (File, std::io::Result<(Vec<u8>, usize)>) {
                    // 2. Loop to fill the buffer
                    loop {
                        // 3. Read into the *remaining* part of the buffer
                        let bytes_read = match sftp_file_ref.read(&mut chunk_buffer[bytes_in_buffer..]) {
                            Ok(0) => {
                                // End of File. Stop reading.
                                // We will return the partial buffer (if any).
                                break; 
                            },
                            Ok(n) => n, // We read n bytes
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue, // Retry on interrupt
                            Err(e) => return (sftp_file_ref, Err(e)), // A real error
                        };

                        bytes_in_buffer += bytes_read;

                        if bytes_in_buffer == chunk_buffer.len() {
                            // Buffer is full, this chunk is done.
                            break;
                        }
                        // If buffer is not full, and not EOF, loop again to read more.
                    }

                    // 4. Return the file, the buffer, and the total bytes we read
                    (sftp_file_ref, Ok((chunk_buffer, bytes_in_buffer)))
                }
            }).await?;

            // Re-acquire ownership for the next loop iteration
            sftp_file = file_out;
            let (mut buffer, bytes_read) = read_result?; // Handle the IO::Result

            // 5. Check (End of File)
            if bytes_read == 0 {
                println!("[Worker] ({}) End of file reached.", cfg.bank_remote_path);
                break; // EOF
            }

            let chunk_data = Bytes::copy_from_slice(&buffer[..bytes_read]);
            
            // This log line will now show your chunk size (or smaller, for the last part)
            println!("[Worker] ({}) Read chunk of {} bytes for part {}", cfg.bank_remote_path, bytes_read, part_number);

            buffer.truncate(bytes_read);
            // 6. Upload Part
            let part_resp = s3_client
                .upload_part()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .upload_id(upload_id)
                .part_number(part_number)
                // .body(ByteStream::from(chunk_data))
                .body(ByteStream::from(buffer))
                .send()
                .await
                .context(format!("S3 UploadPart {} failed", part_number))?;
            
            let etag = part_resp.e_tag().ok_or_else(|| anyhow!("S3 response missing ETag for part {}", part_number))?;
            
            println!("[Worker] ({}) Uploaded part {}", cfg.bank_remote_path, part_number);

            completed_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build(),
            );

            // 7. Increment
            part_number += 1;
        }

         // 8. Finalize Transfer
         // Handle 0-byte files: if completed_parts is empty, we can't complete.
        if !completed_parts.is_empty() {
            println!("[Worker] ({}) Completing multipart upload...", cfg.bank_remote_path);
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
        } else {
             // This case handles 0-byte files
             println!("[Worker] ({}) File was empty, aborting MPU and creating empty object.", cfg.bank_remote_path);
             
             // Abort the MPU
             s3_client
                .abort_multipart_upload()
                .bucket(cfg.storage_bucket.clone())
                .key(cfg.bank_remote_path.clone())
                .upload_id(upload_id)
                .send()
                .await
                .context("Failed to abort MPU for empty file")?;
            
            // Upload a single 0-byte object
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
        // Transfer failed, we MUST abort the MPU
        eprintln!("[Worker] ({}) Aborting MPU due to error: {}", cfg.bank_remote_path, e);
        if let Err(abort_err) = s3_client
            .abort_multipart_upload()
            .bucket(cfg.storage_bucket.clone())
            .key(cfg.bank_remote_path.clone())
            .upload_id(upload_id)
            .send()
            .await {
                eprintln!("[Worker] ({}) CRITICAL: Failed to abort MPU: {}", cfg.bank_remote_path, abort_err);
            }
    }
    
    transfer_result
}
