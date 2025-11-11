use std::net::TcpStream;
use anyhow::{Result, Context, anyhow};
use std::env;
use std::io::Read;
use ssh2::{Session, File};
use std::path::Path;

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
//env UPLOAD_CHUNK_SIZE_MB=10 time -l ./target/release/sftp_puller_main

use std::process;
use std::thread;
use std::time::Duration;


#[derive(Debug, Clone)] // Added Clone and Debug
struct Config {
    bank_host: String,
    bank_port: u16,
    bank_username: String,
    bank_password: String,
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
            bank_password: env::var("BANK_PASSWORD")?,
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
    let sftp = create_sftp_session(&cfg).await?;
    let s3_client = create_s3_client(&cfg).await?;

    // Download the file and upload to object storage
    upload_to_storage(&cfg, sftp, s3_client).await?;

    println!("Successfully transferred file.");
    Ok(())
}

async fn create_sftp_session(cfg: &Config) -> Result<ssh2::Sftp> {
    let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))?;
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;

    // Authenticate using username & password
    sess.userauth_password(&cfg.bank_username, &cfg.bank_password)?;
    println!(" Authenticated successfully!");


    if !sess.authenticated() {
        return Err(anyhow::anyhow!("Authentication failed"));
    }
  
    // Open SFTP channel
    let sftp = sess.sftp()?;
    println!(" SFTP session established!");

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


// --- THIS IS THE UPDATED FUNCTION ---

async fn upload_to_storage(
    cfg: &Config,
    sftp: ssh2::Sftp,
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
            move || -> Result<File> {
                sftp.open(Path::new(&sftp_path)).context(format!("SFTP file open failed: {}", sftp_path))
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




// use std::net::TcpStream;
// use anyhow::{Result, Context};
// use std::env;
// use std::io::Read;
// use ssh2::{Session, File};

// struct Config {
//     bank_host: String,
//     bank_port: u16,
//     bank_username: String,
//     bank_password: String,
//     bank_remote_path: String,

//     storage_endpoint: String,
//     storage_access_key: String,
//     storage_secret_key: String,
//     storage_bucket: String,
//     storage_region: String,

//     upload_chunk_size: usize, // e.g., 10 * 1024 * 1024 for 10MB

// }

// impl Config {
//     fn from_env() -> Result<Self> {
//         dotenv::dotenv().ok();

//         Ok(Self {
//             bank_host: env::var("BANK_HOST")?,
//             bank_port: env::var("BANK_PORT")?.parse()?,
//             bank_username: env::var("BANK_USERNAME")?,
//             bank_password: env::var("BANK_PASSWORD")?,
//             bank_remote_path: env::var("BANK_REMOTE_PATH")?,
//             storage_endpoint: env::var("STORAGE_ENDPOINT")?,
//             storage_access_key: env::var("STORAGE_ACCESS_KEY")?,
//             storage_secret_key: env::var("STORAGE_SECRET_KEY")?,
//             storage_bucket: env::var("STORAGE_BUCKET")?,
//             storage_region: env::var("STORAGE_REGION")?,
//             upload_chunk_size: env::var("UPLOAD_CHUNK_SIZE_MB").unwrap_or_else(|_| "10".to_string()).parse::<usize>()? * 1024*1024 ,

//         })
//     }
// }
 
 
//  #[tokio::main]
// async fn main() -> Result<()> {   
//     let cfg = Config::from_env()?;

//     // Connect to the bank sftp server via SSH
//     let sftp = create_sftp_session(&cfg).await?;
//     let s3_client = create_s3_client(&cfg).await?;

//     // Download the file and upload to object storage
//     upload_to_storage(&cfg, sftp, s3_client).await?;

//     // docker cp transactions.csv test-sftp:/home/testuser/upload/transactions.csv                  

//     Ok(())
// }

// async fn create_sftp_session(cfg: &Config) -> Result<ssh2::Sftp> {
//     let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))?;
//     let mut sess = Session::new()?;
//     sess.set_tcp_stream(tcp);
//     sess.handshake()?;

//     // Authenticate using username & password
//     sess.userauth_password(&cfg.bank_username, &cfg.bank_password)?;
//     println!(" Authenticated successfully!");


//     if !sess.authenticated() {
//         return Err(anyhow::anyhow!("Authentication failed"));
//     }
  
//     // Open SFTP channel
//     let sftp = sess.sftp()?;
//     println!(" SFTP session established!");

//     Ok(sftp)
// }

// use aws_config::{self, imds::client};
// use aws_credential_types::provider::SharedCredentialsProvider;
// use aws_credential_types::Credentials;
// use aws_sdk_s3::{config::Builder, primitives::ByteStream, Client as S3Client,
//     types::{CompletedMultipartUpload, CompletedPart},
// };
// use aws_types::region::Region;

// async fn create_s3_client(cfg: &Config) -> Result<S3Client> {
//     let region = Region::new(cfg.storage_region.clone());

//     let base_config = aws_config::from_env()
//         .region(region.clone())
//         .load()
//         .await;

//     let credentials = Credentials::new(
//         &cfg.storage_access_key,
//         &cfg.storage_secret_key,
//         None,
//         None,
//         "custom",
//     );

//     let credentials_provider = SharedCredentialsProvider::new(credentials);

//     let s3_config = Builder::from(&base_config)
//         .region(region)
//         .endpoint_url(cfg.storage_endpoint.clone())
//         .credentials_provider(credentials_provider)
//         .build();

//     let client = S3Client::from_conf(s3_config);

//     Ok(client)
// }





// // --- New imports needed for streaming ---

// use bytes::Bytes;
// use tokio::sync::mpsc; // The async channel for our "pipe"
// use tokio_stream::{wrappers::ReceiverStream, StreamExt};
// use anyhow::anyhow;
// use std::path::{Path, PathBuf};

// async fn upload_to_storage(
//     cfg: &Config,
//     sftp: ssh2::Sftp,
//     s3_client: S3Client,
// ) -> Result<()> {
    
//     // 1. Initiate Upload
//     let mpu = s3_client
//         .create_multipart_upload()
//         .bucket(cfg.storage_bucket.clone())
//         .key(cfg.bank_remote_path.clone())
//         .send()
//         .await?;


//     let upload_id = mpu.upload_id().ok_or_else(|| anyhow!("S3 response missing UploadId"))?;
//     println!(" Initiated MPU. UploadId: {}", upload_id);

//     // This block ensures we abort the MPU on any error
//     let transfer_result = async { 
//         // 2. Store State (In-Memory)
//         let mut part_number = 1; // This is the in-memory offset
//         let mut completed_parts = Vec::new();

//         // 3. Connect to SFTP (in a blocking task)
//         let mut sftp_file = tokio::task::spawn_blocking({
//             let cfg_clone = cfg.clone();
//             let sftp_path = cfg.bank_remote_path.clone();
//             move || -> Result<File> {
//                 sftp.open(Path::new(&sftp_path)).context(format!("SFTP file open failed: {}", sftp_path))
//             }
//         }).await??; // Double ?? - one for spawn_blocking, one for inner Result

//         let mut buffer = vec![0u8; cfg.upload_chunk_size];


//        // 4. Stream Parts
//         loop {
//             // Read chunk in blocking task
//             // We move file and buffer into the task, and get them back
//             let (file_out, buffer_out, bytes_read_result) = tokio::task::spawn_blocking({
//                 let mut sftp_file_ref = sftp_file; // Take ownership
//                 let mut buffer_ref = buffer;    // Take ownership
//                 move || -> (File, Vec<u8>, std::io::Result<usize>) {
//                      let bytes_read = sftp_file_ref.read(&mut buffer_ref[..]);
//                      (sftp_file_ref, buffer_ref, bytes_read) // Return ownership
//                 }
//             }).await?;

//             // Re-acquire ownership for the next loop iteration
//             sftp_file = file_out;
//             buffer = buffer_out;
            
//             let bytes_read = bytes_read_result?; // Handle the IO::Result

//             // 5. Check (End of File)
//             if bytes_read == 0 {
//                 break; // EOF
//             }

//             let chunk_data = Bytes::copy_from_slice(&buffer[..bytes_read]);
//             println!("[Worker ] ({}) Read chunk of {} bytes for part {}", cfg.bank_remote_path, bytes_read, part_number);

//             // 6. Upload Part
//             let part_resp = s3_client
//                 .upload_part()
//                 .bucket(cfg.storage_bucket.clone())
//                 .key(cfg.bank_remote_path.clone()) // <-- FIXED: Use job.storage_key
//                 .upload_id(upload_id)
//                 .part_number(part_number)
//                 .body(ByteStream::from(chunk_data))
//                 .send()
//                 .await
//                 .context(format!("S3 UploadPart {} failed", part_number))?;
            
//             let etag = part_resp.e_tag().ok_or_else(|| anyhow!("S3 response missing ETag for part {}", part_number))?;
            
//             // <-- FIXED: Use worker_id and job.sftp_path
//             println!("[Worker ] ({}) Uploaded part {}",  cfg.bank_remote_path, part_number);

//             completed_parts.push(
//                 CompletedPart::builder()
//                     .part_number(part_number)
//                     .e_tag(etag)
//                     .build(),
//             );

//             // 7. Increment
//             part_number += 1;
//         }

//          // 8. Finalize Transfer
//         let mpu_completion = CompletedMultipartUpload::builder()
//             .set_parts(Some(completed_parts))
//             .build();

//         s3_client
//             .complete_multipart_upload()
//             .bucket(cfg.storage_bucket.clone())
//             .key(cfg.bank_remote_path.clone())
//             .upload_id(upload_id)
//             .multipart_upload(mpu_completion)
//             .send()
//             .await
//             .context("Failed to complete multipart upload")?;

        


//         Ok(())

//      }.await;

//       if let Err(e) = &transfer_result {
//         // Transfer failed, we MUST abort the MPU
//         eprintln!("[Worker ] ({}) Aborting MPU due to error: {}", cfg.bank_remote_path, e);
//         if let Err(abort_err) = s3_client
//             .abort_multipart_upload()
//             .bucket(cfg.storage_bucket.clone())
//             .key(cfg.bank_remote_path.clone())
//             .upload_id(upload_id)
//             .send()
//             .await {
//                 eprintln!("[Worker ] ({}) CRITICAL: Failed to abort MPU: {}", cfg.bank_remote_path, abort_err);
//             }
//     }

    
   

//     transfer_result



// }