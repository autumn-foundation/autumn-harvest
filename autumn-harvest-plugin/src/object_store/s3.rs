//! S3 [`ObjectBackend`] on `aws-sdk-s3` (issue #1983).
//!
//! The module re-exports [`aws_sdk_s3`], so an application needs no direct
//! dependency on it. Any S3-compatible store works, for example MinIO. For a
//! store that is not AWS, set `endpoint_url` and `force_path_style(true)` on
//! the client config.
//!
//! ```text
//! use autumn_harvest_plugin::object_store::s3::{S3Backend, aws_sdk_s3};
//!
//! let client = aws_sdk_s3::Client::new(&aws_config::load_from_env().await);
//! let backend = Arc::new(S3Backend::new(client, "harvest-archive"));
//! ```

use aws_sdk_s3::error::{DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::primitives::ByteStream;

/// The AWS SDK for S3, re-exported so that an application can build a
/// client with no direct dependency.
pub use aws_sdk_s3;

use super::{ObjectBackend, ObjectFuture, ObjectStoreError};

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
        Box::pin(async move {
            let output = match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(output) => output,
                Err(SdkError::ServiceError(err))
                    if matches!(err.err(), GetObjectError::NoSuchKey(_)) =>
                {
                    return Ok(None);
                }
                Err(err) => return Err(s3_error("get", key, &err)),
            };
            let bytes = output
                .body
                .collect()
                .await
                .map_err(|err| s3_error("get", key, &err))?;
            Ok(Some(bytes.into_bytes().to_vec()))
        })
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
