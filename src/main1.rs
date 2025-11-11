use anyhow::{anyhow, Context, Result};
use aws_config;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client as S3Client,
};
use aws_types::region::Region;
use std::env;
use std::io::{Read, ErrorKind};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ssh2::{Session, Sftp, File};

use rusqlite::{params, Connection as DbConnection, Transaction};

// ---
// 1. Configuration
// ---

/// Application configuration loaded from environment variables.
#[derive(Debug, Clone)]
struct Config {
    bank_host: String,
    bank_port: u16,
    bank_username: String,
    bank_password: String,
    bank_root_path: String, // e.g., "/uploads_to_center"
    bank_id: String,      // e.g., "bank_A"

    storage_endpoint: String,
    storage_access_key: String,
    storage_secret_key: String,
    storage_bucket: String,
    storage_region: String,

    db_path: String,
    worker_count: usize,
    scan_interval_sec: u64,
    upload_chunk_size: usize, // e.g., 10 * 1024 * 1024 for 10MB
}

impl Config {
    /// Loads configuration from environment variables.
    fn from_env() -> Result<Self> {
        dotenv::dotenv().ok();
        Ok(Self {
            bank_host: env::var("BANK_HOST").context("BANK_HOST not set")?,
            bank_port: env::var("BANK_PORT").context("BANK_PORT not set")?.parse()?,
            bank_username: env::var("BANK_USERNAME").context("BANK_USERNAME not set")?,
            bank_password: env::var("BANK_PASSWORD").context("BANK_PASSWORD not set")?,
            bank_root_path: env::var("BANK_ROOT_PATH").context("BANK_ROOT_PATH not set")?,
            bank_id: env::var("BANK_ID").context("BANK_ID not set")?,

            storage_endpoint: env::var("STORAGE_ENDPOINT").context("STORAGE_ENDPOINT not set")?,
            storage_access_key: env::var("STORAGE_ACCESS_KEY").context("STORAGE_ACCESS_KEY not set")?,
            storage_secret_key: env::var("STORAGE_SECRET_KEY").context("STORAGE_SECRET_KEY not set")?,
            storage_bucket: env::var("STORAGE_BUCKET").context("STORAGE_BUCKET not set")?,
            storage_region: env::var("STORAGE_REGION").context("STORAGE_REGION not set")?,

            db_path: env::var("DB_PATH").unwrap_or_else(|_| "transfer.db".to_string()),
            worker_count: env::var("WORKER_COUNT").unwrap_or_else(|_| "8".to_string()).parse()?,
            scan_interval_sec: env::var("SCAN_INTERVAL_SEC").unwrap_or_else(|_| "300".to_string()).parse()?,
            upload_chunk_size: env::var("UPLOAD_CHUNK_SIZE_MB").unwrap_or_else(|_| "10".to_string()).parse::<usize>()? * 1024 * 1024,
        })
    }
}

// ---
// 2. Database (Progress Tracker)
// ---

/// Represents a job to be processed.
#[derive(Debug)]
struct TransferJob {
    sftp_path: String,
    storage_key: String,
}

/// A thread-safe handle to the SQLite database.
type Db = Arc<Mutex<DbConnection>>;

/// Initializes the database and creates the `transfer_jobs` table.
fn init_db(db_path: &str) -> Result<Db> {
    let conn = DbConnection::open(db_path).context("Failed to open database")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS transfer_jobs (
            sftp_path     TEXT PRIMARY KEY,
            storage_key   TEXT NOT NULL,
            status        TEXT NOT NULL, -- 'PENDING', 'RUNNING', 'FAILED', 'COMPLETE'
            last_error    TEXT
        )",
        [],
    )?;
    Ok(Arc::new(Mutex::new(conn)))
}

/// On startup, reset any 'RUNNING' jobs to 'PENDING'.
fn cleanup_running_jobs(db: &Db) -> Result<()> {
    let conn = db.lock().unwrap();
    let count = conn.execute(
        "UPDATE transfer_jobs SET status = 'PENDING', last_error = 'Reset from RUNNING on startup' WHERE status = 'RUNNING'",
        [],
    )?;
    if count > 0 {
        println!("[DB] Reset {} 'RUNNING' jobs to 'PENDING'", count);
    }
    Ok(())
}

/// Finds a 'PENDING' or 'FAILED' job and transactionally claims it by
/// setting its status to 'RUNNING'.
fn fetch_and_claim_job(db: &Db) -> Result<Option<TransferJob>> {
    let mut conn = db.lock().unwrap();
    
    // Use a transaction to find and claim the job atomically
    let tx = conn.transaction()?;
    
    let job_opt = tx.query_row(
        "SELECT sftp_path, storage_key FROM transfer_jobs 
         WHERE status = 'PENDING' OR status = 'FAILED' 
         LIMIT 1",
        [],
        |row| {
            Ok(TransferJob {
                sftp_path: row.get(0)?,
                storage_key: row.get(1)?,
            })
        },
    );

    match job_opt {
        Ok(job) => {
            tx.execute(
                "UPDATE transfer_jobs SET status = 'RUNNING', last_error = NULL WHERE sftp_path = ?",
                params![&job.sftp_path],
            )?;
            tx.commit()?;
            Ok(Some(job))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            // No job found, this is not an error
            Ok(None)
        }
        Err(e) => {
            // A real error occurred
            Err(e.into())
        }
    }
}

/// Inserts a new job with 'PENDING' status.
/// Uses ON CONFLICT to avoid duplicates and not overwrite 'COMPLETE' jobs.
fn insert_job(tx: &Transaction, sftp_path: &str, storage_key: &str) -> Result<()> {
    tx.execute(
        "INSERT INTO transfer_jobs (sftp_path, storage_key, status) 
         VALUES (?1, ?2, 'PENDING') 
         ON CONFLICT(sftp_path) DO NOTHING",
        params![sftp_path, storage_key],
    )?;
    Ok(())
}

/// Updates a job's final status.
fn update_job_status(db: &Db, sftp_path: &str, status: &str, error: Option<&str>) -> Result<()> {
    let conn = db.lock().unwrap();
    conn.execute(
        "UPDATE transfer_jobs SET status = ?1, last_error = ?2 WHERE sftp_path = ?3",
        params![status, error, sftp_path],
    )?;
    Ok(())
}


// ---
// 3. SFTP & S3 Client Creation
// ---

/// Creates an S3-compatible client.
async fn create_s3_client(cfg: &Config) -> Result<S3Client> {
    let region = Region::new(cfg.storage_region.clone());
    let base_config = aws_config::from_env().region(region.clone()).load().await;

    let credentials = Credentials::new(
        &cfg.storage_access_key,
        &cfg.storage_secret_key,
        None,
        None,
        "custom",
    );
    let credentials_provider = SharedCredentialsProvider::new(credentials);

    let s3_config = aws_sdk_s3::config::Builder::from(&base_config)
        .region(region)
        .endpoint_url(cfg.storage_endpoint.clone())
        .credentials_provider(credentials_provider)
        .build();

    Ok(S3Client::from_conf(s3_config))
}

/// Creates a new, blocking SFTP session.
fn create_sftp_session(cfg: &Config) -> Result<Sftp> {
    let tcp = TcpStream::connect(format!("{}:{}", cfg.bank_host, cfg.bank_port))?;
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;

    sess.userauth_password(&cfg.bank_username, &cfg.bank_password)?;

    if !sess.authenticated() {
        return Err(anyhow::anyhow!("SFTP Authentication failed"));
    }
    
    let sftp = sess.sftp()?;
    Ok(sftp)
}

// ---
// 4. Scanner (Phase 1)
// ---

/// Task that periodically scans the SFTP server for new jobs.
async fn scanner_task(cfg: Arc<Config>, db: Db) {
    println!("[Scanner] Started. Will scan every {}s", cfg.scan_interval_sec);
    let scan_duration = Duration::from_secs(cfg.scan_interval_sec);

    loop {
        println!("[Scanner] Starting new scan...");
        
        let scan_result = tokio::task::spawn_blocking({
            let cfg_clone = cfg.clone();
            let db_clone = db.clone();
            move || -> Result<()> {
                let sftp = create_sftp_session(&cfg_clone)?;
                println!("[Scanner] SFTP connected. Scanning path: {}", cfg_clone.bank_root_path);

                let mut conn = db_clone.lock().unwrap();
                let tx = conn.transaction()?;

                // This function will recursively scan for _SUCCESS files
                scan_sftp_recursive(&sftp, &tx, &cfg_clone, Path::new(&cfg_clone.bank_root_path))?;

                tx.commit()?;
                Ok(())
            }
        }).await;

        match scan_result {
            Ok(Ok(_)) => println!("[Scanner] Scan finished successfully."),
            Ok(Err(e)) => eprintln!("[Scanner] Error during scan: {:?}", e),
            Err(e) => eprintln!("[Scanner] Scan task panicked: {:?}", e),
        }

        tokio::time::sleep(scan_duration).await;
    }
}

/// Recursively scans SFTP directories for `_SUCCESS` files and adds jobs.
fn scan_sftp_recursive(sftp: &Sftp, tx: &Transaction, cfg: &Config, path: &Path) -> Result<()> {
    let entries = match sftp.readdir(path) {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!("[Scanner] Failed to read dir {:?}: {}", path, e);
            return Ok(()); // Don't kill the whole scan
        }
    };

    let mut has_success_file = false;
    let mut files_in_dir = Vec::new();
    let mut sub_dirs = Vec::new();

    for (entry_path, stat) in entries {
        if stat.is_dir() {
            sub_dirs.push(entry_path);
        } else {
            let file_name = entry_path.file_name().unwrap_or_default().to_str().unwrap_or_default();
            if file_name == "_SUCCESS" {
                has_success_file = true;
            } else if file_name.ends_with(".csv.gz") {
                files_in_dir.push(entry_path);
            }
        }
    }

    if has_success_file {
        // This is a "valid batch", add all .csv.gz files as jobs
        println!("[Scanner] Found _SUCCESS in {:?}. Registering {} files.", path, files_in_dir.len());
        for file_path in files_in_dir {
            let sftp_path = file_path.to_str().unwrap();
            let storage_key = map_s3_key(cfg, sftp_path)?;
            insert_job(tx, sftp_path, &storage_key)?;
        }
    } else {
        // No _SUCCESS, so recurse into subdirectories
        for dir_path in sub_dirs {
            scan_sftp_recursive(sftp, tx, cfg, &dir_path)?;
        }
    }

    Ok(())
}

/// Maps the full SFTP path to the target S3 key.
fn map_s3_key(cfg: &Config, sftp_path_str: &str) -> Result<String> {
    let sftp_path = Path::new(sftp_path_str);
    let root_path = Path::new(&cfg.bank_root_path);
    
    let relative_path = sftp_path.strip_prefix(root_path)
        .map_err(|e| anyhow!("Failed to strip prefix '{}' from '{}': {}", root_path.display(), sftp_path.display(), e))?;

    let s3_key = Path::new(&format!("bank_id={}", cfg.bank_id))
        .join(relative_path)
        .to_str()
        .unwrap()
        .replace("\\", "/"); // Ensure forward slashes for S3

    Ok(s3_key)
}


// ---
// 5. Worker (Phase 2)
// ---

/// A worker task that continuously fetches and processes jobs.
async fn worker_task(id: usize, cfg: Arc<Config>, db: Db, s3_client: Arc<S3Client>) {
    println!("[Worker {}] Started.", id);
    loop {
        // 1. Fetch and Claim Job
        let job = match tokio::task::spawn_blocking({
            let db_clone = db.clone();
            move || fetch_and_claim_job(&db_clone)
        }).await {
            Ok(Ok(Some(job))) => job,
            Ok(Ok(None)) => {
                // No jobs available, sleep for a bit
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            Ok(Err(e)) => {
                eprintln!("[Worker {}] DB error fetching job: {:?}", id, e);
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
            Err(e) => {
                eprintln!("[Worker {}] Task panic fetching job: {:?}", id, e);
                continue;
            }
        };

        println!("[Worker {}] Claimed job: {}", id, job.sftp_path);

        // 2. Process File Transfer
        let transfer_result = transfer_file(id, cfg.clone(), s3_client.clone(), &job).await;

        // 3. Update Status
        let final_status = match transfer_result {
            Ok(_) => {
                println!("[Worker {}] SUCCESS: {}", id, job.sftp_path);
                ("COMPLETE", None)
            }
            Err(e) => {
                let err_msg = e.to_string();
                eprintln!("[Worker {}] FAILED: {}. Error: {}", id, job.sftp_path, err_msg);
                ("FAILED", Some(err_msg))
            }
        };
        
        let (status_str, error_opt) = final_status;
        if let Err(e) = update_job_status(&db, &job.sftp_path, status_str, error_opt.as_deref()) {
            eprintln!("[Worker {}] CRITICAL: Failed to update DB status for {}: {:?}", id, job.sftp_path, e);
        }
    }
}

// ---
// 6. File Transfer (Phase 3)
// ---

/// Implements the core file transfer logic (Phase 3).
async fn transfer_file(
    worker_id: usize,
    cfg: Arc<Config>,
    s3_client: Arc<S3Client>,
    job: &TransferJob,
) -> Result<()> {
    
    // 1. Initiate Upload
    let mpu = s3_client
        .create_multipart_upload()
        .bucket(cfg.storage_bucket.clone())
        .key(job.storage_key.clone())
        .send()
        .await
        .context("Failed to initiate multipart upload")?;

    let upload_id = mpu.upload_id().ok_or_else(|| anyhow!("S3 response missing UploadId"))?;

    println!("[Worker {}] ({}) Initiated MPU. UploadId: {}", worker_id, job.sftp_path, upload_id);

    // This block ensures we abort the MPU on any error
    let transfer_result = async {
        // 2. Store State (In-Memory)
        let mut part_number = 1; // This is the in-memory offset
        let mut completed_parts = Vec::new();

        // 3. Connect to SFTP (in a blocking task)
        let mut sftp_file = tokio::task::spawn_blocking({
            let cfg_clone = cfg.clone();
            let sftp_path = job.sftp_path.clone();
            move || -> Result<File> {
                let sftp = create_sftp_session(&cfg_clone).context("SFTP connection failed for transfer")?;
                sftp.open(Path::new(&sftp_path)).context(format!("SFTP file open failed: {}", sftp_path))
            }
        }).await??; // Double ?? - one for spawn_blocking, one for inner Result

        let mut buffer = vec![0u8; cfg.upload_chunk_size];

        // 4. Stream Parts
        loop {
            // Read chunk in blocking task
            let bytes_read = tokio::task::spawn_blocking({
                let mut sftp_file_ref = &mut sftp_file;
                let mut buffer_ref = &mut buffer[..];
                move || -> std::io::Result<usize> {
                     sftp_file_ref.read(&mut buffer_ref)
                }
            }).await??;

            // 5. Check (End of File)
            if bytes_read == 0 {
                break; // EOF
            }

            let chunk_data = Bytes::copy_from_slice(&buffer[..bytes_read]);

            // 6. Upload Part
            let part_resp = s3_client
                .upload_part()
                .bucket(cfg.storage_bucket.clone())
                .key(job.storage_key.clone())
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(chunk_data))
                .send()
                .await
                .context(format!("S3 UploadPart {} failed", part_number))?;
            
            let etag = part_resp.e_tag().ok_or_else(|| anyhow!("S3 response missing ETag for part {}", part_number))?;
            
            println!("[Worker {}] ({}) Uploaded part {}", worker_id, job.sftp_path, part_number);

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
        let mpu_completion = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        s3_client
            .complete_multipart_upload()
            .bucket(cfg.storage_bucket.clone())
            .key(job.storage_key.clone())
            .upload_id(upload_id)
            .multipart_upload(mpu_completion)
            .send()
            .await
            .context("Failed to complete multipart upload")?;

        Ok(())

    }.await; // End of async block for transfer

    
    if let Err(e) = &transfer_result {
        // Transfer failed, we MUST abort the MPU
        eprintln!("[Worker {}] ({}) Aborting MPU due to error: {}", worker_id, job.sftp_path, e);
        if let Err(abort_err) = s3_client
            .abort_multipart_upload()
            .bucket(cfg.storage_bucket.clone())
            .key(job.storage_key.clone())
            .upload_id(upload_id)
            .send()
            .await {
                eprintln!("[Worker {}] ({}) CRITICAL: Failed to abort MPU: {}", worker_id, job.sftp_path, abort_err);
            }
    }

    transfer_result
}

// ---
// 7. Main Application
// ---

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init(); // Initialize logging
    
    // 1. Load Config
    let cfg = Arc::new(Config::from_env()?);
    println!("[Main] Config loaded. Workers: {}, DB: {}", cfg.worker_count, cfg.db_path);

    // 2. Init DB
    let db = init_db(&cfg.db_path)?;
    println!("[Main] Database initialized at {}", cfg.db_path);

    // 3. Init S3 Client
    let s3_client = Arc::new(create_s3_client(&cfg).await?);
    println!("[Main] S3 client created for endpoint: {}", cfg.storage_endpoint);

    // 4. Run Startup Cleanup
    cleanup_running_jobs(&db)?;

    // 5. Spawn Scanner
    tokio::spawn(scanner_task(cfg.clone(), db.clone()));

    // 6. Spawn Workers
    let mut worker_handles = Vec::new();
    for i in 0..cfg.worker_count {
        let worker_handle = tokio::spawn(worker_task(
            i + 1,
            cfg.clone(),
            db.clone(),
            s3_client.clone(),
        ));
        worker_handles.push(worker_handle);
    }
    
    println!("[Main] {} workers spawned. Application is running.", cfg.worker_count);
    println!("[Main] Press Ctrl+C to shut down.");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    println!("[Main] Shutdown signal received. Exiting.");

    // In a real app, you'd signal tasks to shut down gracefully.
    // For this example, we just exit.
    
    Ok(())
}