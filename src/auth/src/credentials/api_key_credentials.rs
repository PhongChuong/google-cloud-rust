// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{Credentials, Result};
use crate::headers_util::build_api_key_headers;
use crate::token::{Token, TokenProvider};
use http::{Extensions, HeaderMap};
use std::sync::Arc;

struct ApiKeyTokenProvider {
    api_key: String,
}

impl std::fmt::Debug for ApiKeyTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyCredentials")
            .field("api_key", &"[censored]")
            .finish()
    }
}

#[async_trait::async_trait]
impl TokenProvider for ApiKeyTokenProvider {
    async fn token(&self) -> Result<Token> {
        Ok(Token {
            token: self.api_key.clone(),
            token_type: String::new(),
            expires_at: None,
            metadata: None,
        })
    }
}

#[derive(Debug)]
struct ApiKeyCredentials<T>
where
    T: TokenProvider,
{
    token_provider: T,
    quota_project_id: Option<String>,
}

/// A builder for creating credentials that authenticate using an [API key].
///
/// API keys are convenient because no [principal] is needed. The API key
/// associates the request with a Google Cloud project for billing and quota
/// purposes.
///
/// Note that only some Cloud APIs support API keys. The rest require full
/// credentials.
///
/// [API key]: https://cloud.google.com/docs/authentication/api-keys-use
/// [principal]: https://cloud.google.com/docs/authentication#principal
#[derive(Debug)]
pub struct Builder {
    api_key: String,
    quota_project_id: Option<String>,
}

impl Builder {
    /// Creates a new builder with given API key.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::api_key_credentials::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::new("my-api-key")
    ///     .build();
    /// # });
    /// ```
    pub fn new<T: Into<String>>(api_key: T) -> Self {
        Self {
            api_key: api_key.into(),
            quota_project_id: None,
        }
    }

    /// Sets the [quota project] for these credentials.
    ///
    /// In some services, you can use an account in one project for authentication
    /// and authorization, and charge the usage to a different project. This requires
    /// that the user has `serviceusage.services.use` permissions on the quota project.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_auth::credentials::api_key_credentials::Builder;
    /// # tokio_test::block_on(async {
    /// let credentials = Builder::new("my-api-key")
    ///     .with_quota_project_id("my-project")
    ///     .build();
    /// # });
    /// ```
    ///
    /// [quota project]: https://cloud.google.com/docs/quotas/quota-project
    pub fn with_quota_project_id<T: Into<String>>(mut self, quota_project_id: T) -> Self {
        self.quota_project_id = Some(quota_project_id.into());
        self
    }

    /// Returns a [Credentials] instance with the configured settings.
    pub fn build(self) -> Credentials {
        let token_provider = ApiKeyTokenProvider {
            api_key: self.api_key,
        };

        let quota_project_id = std::env::var("GOOGLE_CLOUD_QUOTA_PROJECT")
            .ok()
            .or(self.quota_project_id);

        Credentials {
            inner: Arc::new(ApiKeyCredentials {
                token_provider,
                quota_project_id,
            }),
        }
    }
}

#[async_trait::async_trait]
impl<T> CredentialsProvider for ApiKeyCredentials<T>
where
    T: TokenProvider,
{
    async fn token(&self, _extensions: Extensions) -> Result<Token> {
        self.token_provider.token().await
    }

    async fn headers(&self, extensions: Extensions) -> Result<HeaderMap> {
        let token = self.token(extensions).await?;
        build_api_key_headers(&token, &self.quota_project_id)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::credentials::QUOTA_PROJECT_KEY;
    use http::HeaderValue;
    use scoped_env::ScopedEnv;

    const API_KEY_HEADER_KEY: &str = "x-goog-api-key";

    #[test]
    fn debug_token_provider() {
        let expected = ApiKeyTokenProvider {
            api_key: "super-secret-api-key".to_string(),
        };
        let fmt = format!("{expected:?}");
        assert!(!fmt.contains("super-secret-api-key"), "{fmt}");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_basic() {
        let _e = ScopedEnv::remove("GOOGLE_CLOUD_QUOTA_PROJECT");

        let creds = Builder::new("test-api-key").build();
        let token = creds.token(Extensions::new()).await.unwrap();
        assert_eq!(
            token,
            Token {
                token: "test-api-key".to_string(),
                token_type: String::new(),
                expires_at: None,
                metadata: None,
            }
        );
        let headers = creds.headers(Extensions::new()).await.unwrap();
        let value = headers.get(API_KEY_HEADER_KEY).unwrap();

        assert_eq!(headers.len(), 1, "{headers:?}");
        assert_eq!(value, HeaderValue::from_static("test-api-key"));
        assert!(value.is_sensitive());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_with_options() {
        let _e = ScopedEnv::remove("GOOGLE_CLOUD_QUOTA_PROJECT");

        let creds = Builder::new("test-api-key")
            .with_quota_project_id("qp-option")
            .build();
        let headers = creds.headers(Extensions::new()).await.unwrap();
        let api_key = headers.get(API_KEY_HEADER_KEY).unwrap();
        let quota_project = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2, "{headers:?}");
        assert_eq!(api_key, HeaderValue::from_static("test-api-key"));
        assert!(api_key.is_sensitive());
        assert_eq!(quota_project, HeaderValue::from_static("qp-option"));
        assert!(!quota_project.is_sensitive());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_api_key_credentials_with_env() {
        let _e = ScopedEnv::set("GOOGLE_CLOUD_QUOTA_PROJECT", "qp-env");

        let creds = Builder::new("test-api-key")
            .with_quota_project_id("qp-option")
            .build();
        let headers = creds.headers(Extensions::new()).await.unwrap();
        let api_key = headers.get(API_KEY_HEADER_KEY).unwrap();
        let quota_project = headers.get(QUOTA_PROJECT_KEY).unwrap();

        assert_eq!(headers.len(), 2, "{headers:?}");
        assert_eq!(api_key, HeaderValue::from_static("test-api-key"));
        assert!(api_key.is_sensitive());
        assert_eq!(quota_project, HeaderValue::from_static("qp-env"));
        assert!(!quota_project.is_sensitive());
    }
}
