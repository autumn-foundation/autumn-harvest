//! S3 [`ObjectBackend`] on `aws-sdk-s3` (issue #1983).
//!
//! The module re-exports [`aws_sdk_s3`]. An application still needs
//! `aws-config` to load credentials. Any S3-compatible store works, for
//! example `MinIO`.
//!
//! Grant `s3:ListBucket` as well as `s3:GetObject`. Without it, AWS answers a
//! read of a missing key with 403, and the backend reports an error, not a
//! missing object.
//!
//! For a store that is not AWS, set `endpoint_url` and `force_path_style(true)`
//! on the client config.
//!
//! ```text
//! use autumn_harvest_plugin::object_store::s3::{S3Backend, aws_sdk_s3};
//!
//! let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
//! let client = aws_sdk_s3::Client::new(&config);
//! let backend = Arc::new(S3Backend::new(client, "harvest-archive"));
//! ```

use aws_sdk_s3::error::{DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;

/// The AWS SDK for S3, re-exported so that an application can build a
/// client with no direct dependency.
pub use aws_sdk_s3;

use super::{ObjectBackend, ObjectFuture, ObjectStoreError, check_size};

/// One S3 bucket.
#[derive(Debug, Clone)]
pub struct S3Backend {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3Backend {
    /// Use `bucket` through `client`. The bucket must exist.
    #[must_use]
    pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }
}

impl S3Backend {
    /// Read `key`. With a limit, check the object size before the body.
    async fn read(
        &self,
        key: &str,
        max_bytes: Option<u64>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(err) if is_missing_object(&err) => return Ok(None),
            Err(err) => return Err(s3_error("get", key, &err)),
        };
        if let Some(max_bytes) = max_bytes {
            let size = output.content_length().unwrap_or(0);
            check_size(key, u64::try_from(size).unwrap_or(0), max_bytes)?;
        }
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|err| s3_error("get", key, &err))?
            .into_bytes()
            .to_vec();
        if let Some(max_bytes) = max_bytes {
            check_size(key, bytes.len() as u64, max_bytes)?;
        }
        Ok(Some(bytes))
    }
}

/// Whether a `GetObject` error means that no object is at the key.
///
/// Some S3-compatible stores send a bare 404 with no `NoSuchKey` code. A 404
/// for a missing bucket stays an error, because that is a configuration fault.
fn is_missing_object(
    err: &SdkError<GetObjectError, aws_sdk_s3::config::http::HttpResponse>,
) -> bool {
    use aws_sdk_s3::error::ProvideErrorMetadata as _;
    let Some(service) = err.as_service_error() else {
        return false;
    };
    if matches!(service, GetObjectError::NoSuchKey(_)) {
        return true;
    }
    err.raw_response()
        .is_some_and(|raw| raw.status().as_u16() == 404)
        && service.code() != Some("NoSuchBucket")
}

fn s3_error<E>(op: &str, key: &str, err: &E) -> ObjectStoreError
where
    E: std::error::Error,
{
    ObjectStoreError(format!(
        "S3 {op} of {key} failed: {}",
        DisplayErrorContext(err)
    ))
}

impl ObjectBackend for S3Backend {
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> ObjectFuture<'a, ()> {
        Box::pin(async move {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .content_type(content_type)
                .body(ByteStream::from(bytes))
                .send()
                .await
                .map_err(|err| s3_error("put", key, &err))?;
            Ok(())
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> ObjectFuture<'a, Option<Vec<u8>>> {
        Box::pin(self.read(key, None))
    }

    fn get_bounded<'a>(
        &'a self,
        key: &'a str,
        max_bytes: u64,
    ) -> ObjectFuture<'a, Option<Vec<u8>>> {
        Box::pin(self.read(key, Some(max_bytes)))
    }

    fn delete<'a>(&'a self, key: &'a str) -> ObjectFuture<'a, ()> {
        // S3 reports success when no object is at the key.
        Box::pin(async move {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|err| s3_error("delete", key, &err))?;
            Ok(())
        })
    }
}
