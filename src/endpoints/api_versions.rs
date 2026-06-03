use crate::protocol_types::api_key::ApiKey;
use crate::protocol_types::api_versions::{ApiKeyEntry, ApiVersionsResponse};

pub fn handle_api_versions_request(version: i16) -> ApiVersionsResponse {
    let error_code = if version < 0 || version > 4 { 35 } else { 0 };
    let api_keys = ApiKey::all()
        .iter()
        .map(|&key| {
            let (min_version, max_version) = key.version_range();
            ApiKeyEntry { api_key: key as i16, min_version, max_version }
        })
        .collect();
    ApiVersionsResponse { error_code, api_keys, throttle_time_ms: 0 }
}
