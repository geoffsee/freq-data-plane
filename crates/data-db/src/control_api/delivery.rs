use duckdb::{params, Connection, Error, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryChannelType {
    S3,
    Http,
    Email,
    Webhook,
    Sftp,
    Api,
    Internal,
}

impl DeliveryChannelType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Http => "http",
            Self::Email => "email",
            Self::Webhook => "webhook",
            Self::Sftp => "sftp",
            Self::Api => "api",
            Self::Internal => "internal",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "s3" => Ok(Self::S3),
            "http" => Ok(Self::Http),
            "email" => Ok(Self::Email),
            "webhook" => Ok(Self::Webhook),
            "sftp" => Ok(Self::Sftp),
            "api" => Ok(Self::Api),
            "internal" => Ok(Self::Internal),
            other => Err(Error::InvalidParameterName(format!(
                "invalid delivery channel type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryChannelStatus {
    Active,
    Disabled,
    Deleted,
}

impl DeliveryChannelStatus {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "disabled" => Ok(Self::Disabled),
            "deleted" => Ok(Self::Deleted),
            other => Err(Error::InvalidParameterName(format!(
                "invalid delivery channel status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryChannel {
    pub channel_key: String,
    pub channel_name: String,
    pub channel_type: DeliveryChannelType,
    pub destination_uri: Option<String>,
    pub status: DeliveryChannelStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewDeliveryChannel {
    pub channel_key: String,
    pub channel_name: String,
    pub channel_type: DeliveryChannelType,
    pub destination_uri: Option<String>,
}

pub fn create_delivery_channel(conn: &Connection, channel: &NewDeliveryChannel) -> Result<DeliveryChannel> {
    conn.execute(
        "INSERT INTO control.delivery_channels (
            channel_key, channel_name, channel_type, destination_uri
         ) VALUES (?, ?, ?, ?)",
        params![
            channel.channel_key,
            channel.channel_name,
            channel.channel_type.as_str(),
            channel.destination_uri
        ],
    )?;
    get_delivery_channel_by_key(conn, &channel.channel_key)?.ok_or_else(|| {
        Error::InvalidParameterName("delivery channel insert did not return row".to_string())
    })
}

pub fn get_delivery_channel_by_key(conn: &Connection, channel_key: &str) -> Result<Option<DeliveryChannel>> {
    match conn.query_row(
        "SELECT channel_key, channel_name, channel_type, destination_uri, status
         FROM control.delivery_channels
         WHERE channel_key = ?",
        [channel_key],
        |row| {
            let channel_type: String = row.get(2)?;
            let status: String = row.get(4)?;
            Ok(DeliveryChannel {
                channel_key: row.get(0)?,
                channel_name: row.get(1)?,
                channel_type: DeliveryChannelType::parse(&channel_type)?,
                destination_uri: row.get(3)?,
                status: DeliveryChannelStatus::parse(&status)?,
            })
        },
    ) {
        Ok(channel) => Ok(Some(channel)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryType {
    Import,
    Export,
    Snapshot,
    Share,
    Replication,
    Report,
    Publish,
}

impl DeliveryType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Import => "import",
            Self::Export => "export",
            Self::Snapshot => "snapshot",
            Self::Share => "share",
            Self::Replication => "replication",
            Self::Report => "report",
            Self::Publish => "publish",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "import" => Ok(Self::Import),
            "export" => Ok(Self::Export),
            "snapshot" => Ok(Self::Snapshot),
            "share" => Ok(Self::Share),
            "replication" => Ok(Self::Replication),
            "report" => Ok(Self::Report),
            "publish" => Ok(Self::Publish),
            other => Err(Error::InvalidParameterName(format!("invalid delivery type: {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl DeliveryStatus {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(Error::InvalidParameterName(format!("invalid delivery status: {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delivery {
    pub delivery_id: i64,
    pub delivery_key: String,
    pub delivery_type: DeliveryType,
    pub asset_id: Option<i64>,
    pub database_ref_id: Option<i64>,
    pub status: DeliveryStatus,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewDelivery {
    pub delivery_key: String,
    pub delivery_type: DeliveryType,
    pub asset_id: Option<i64>,
    pub database_ref_id: Option<i64>,
    pub delivery_channel_key: Option<String>,
    pub requested_by_principal_id: Option<i64>,
}

pub fn create_delivery(conn: &Connection, delivery: &NewDelivery) -> Result<Delivery> {
    conn.execute(
        "INSERT INTO control.deliveries (
            delivery_key,
            delivery_type,
            asset_id,
            database_ref_id,
            delivery_channel_id,
            requested_by_principal_id
         ) VALUES (
            ?,
            ?,
            ?,
            ?,
            (SELECT delivery_channel_id FROM control.delivery_channels WHERE channel_key = ?),
            ?
         )",
        params![
            delivery.delivery_key,
            delivery.delivery_type.as_str(),
            delivery.asset_id,
            delivery.database_ref_id,
            delivery.delivery_channel_key,
            delivery.requested_by_principal_id
        ],
    )?;
    get_delivery_by_key(conn, &delivery.delivery_key)?
        .ok_or_else(|| Error::InvalidParameterName("delivery insert did not return row".to_string()))
}

pub fn get_delivery_by_key(conn: &Connection, delivery_key: &str) -> Result<Option<Delivery>> {
    match conn.query_row(
        "SELECT delivery_id, delivery_key, delivery_type, asset_id, database_ref_id, status, error_message
         FROM control.deliveries
         WHERE delivery_key = ?",
        [delivery_key],
        |row| {
            let delivery_type: String = row.get(2)?;
            let status: String = row.get(5)?;
            Ok(Delivery {
                delivery_id: row.get(0)?,
                delivery_key: row.get(1)?,
                delivery_type: DeliveryType::parse(&delivery_type)?,
                asset_id: row.get(3)?,
                database_ref_id: row.get(4)?,
                status: DeliveryStatus::parse(&status)?,
                error_message: row.get(6)?,
            })
        },
    ) {
        Ok(delivery) => Ok(Some(delivery)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}
