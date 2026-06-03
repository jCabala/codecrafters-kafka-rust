#[derive(Clone, Copy)]
pub enum ApiKey {
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

impl ApiKey {
    pub fn version_range(self) -> (i16, i16) {
        match self {
            ApiKey::ApiVersions => (0, 4),
            ApiKey::DescribeTopicPartitions => (0, 0),
        }
    }

    pub fn all() -> &'static [ApiKey] {
        &[ApiKey::ApiVersions, ApiKey::DescribeTopicPartitions]
    }
}

impl TryFrom<i16> for ApiKey {
    type Error = i16;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            18 => Ok(ApiKey::ApiVersions),
            75 => Ok(ApiKey::DescribeTopicPartitions),
            other => Err(other),
        }
    }
}
