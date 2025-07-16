use anyhow::Result;
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_rds::Client as RdsClient;
use log::info;
use std::env;
use tokio::time::{sleep, Duration};

const RDS_MANAGEMENT_ROLE_ENV_KEY: &str = "RDS_MANAGEMENT_ROLE";
const RDS_CLUSTER_ID_ENV_KEY: &str = "RDS_CLUSTER_ID";
const SESSION_NAME: &str = "rds-management-session";

const CLUSTER_STATUS_AVAILABLE: &str = "available";
const CLUSTER_STATUS_STOPPED: &str = "stopped";

pub struct RDSManager {
  cluster_id: String,
  rds_client: RdsClient,
  cached_running_status: Option<bool>,
}

impl RDSManager {
  pub async fn load() -> Option<Self> {
    let role_arn = env::var(RDS_MANAGEMENT_ROLE_ENV_KEY).ok()?;
    let cluster_id = env::var(RDS_CLUSTER_ID_ENV_KEY).ok()?;

    let config = aws_config::from_env().load().await;
    let provider = AssumeRoleProvider::builder(&role_arn)
      .session_name(SESSION_NAME)
      .configure(&config)
      .build()
      .await;

    // Create new config with assumed role credentials
    let rds_config = aws_config::from_env()
      .credentials_provider(provider)
      .load()
      .await;

    let rds_client = RdsClient::new(&rds_config);

    Some(RDSManager {
      cluster_id,
      rds_client,
      cached_running_status: None,
    })
  }

  pub async fn start(&mut self) -> Result<()> {
    if self.is_running().await? {
      return Ok(());
    }

    info!("Starting RDS cluster: {}", self.cluster_id);

    self
      .rds_client
      .start_db_cluster()
      .db_cluster_identifier(&self.cluster_id)
      .send()
      .await?;

    self.wait_for_status(true).await?;

    self.cached_running_status = Some(true);
    info!("Successfully started RDS cluster: {}", self.cluster_id);
    Ok(())
  }

  pub async fn stop(&mut self) -> Result<()> {
    if !self.is_running().await? {
      return Ok(());
    }

    info!("Stopping RDS cluster: {}", self.cluster_id);

    self
      .rds_client
      .stop_db_cluster()
      .db_cluster_identifier(&self.cluster_id)
      .send()
      .await?;

    self.wait_for_status(false).await?;

    self.cached_running_status = Some(false);
    info!("Successfully stopped RDS cluster: {}", self.cluster_id);
    Ok(())
  }

  async fn wait_for_status(&self, target_running: bool) -> Result<()> {
    let target_status = if target_running {
      CLUSTER_STATUS_AVAILABLE
    } else {
      CLUSTER_STATUS_STOPPED
    };
    info!(
      "Waiting for RDS cluster {} to reach status: {}",
      self.cluster_id, target_status
    );

    loop {
      let current_status = self.get_cluster_status().await?;

      if current_status == target_status {
        info!(
          "RDS cluster {} reached target status: {}",
          self.cluster_id, target_status
        );
        break;
      }

      debug!(
        "RDS cluster {} current status: {}, waiting for: {}",
        self.cluster_id, current_status, target_status
      );

      sleep(Duration::from_secs(30)).await;
    }

    Ok(())
  }

  async fn get_cluster_status(&self) -> Result<String> {
    let response = self
      .rds_client
      .describe_db_clusters()
      .db_cluster_identifier(&self.cluster_id)
      .send()
      .await?;

    let response_clusters = response.db_clusters();

    if response_clusters.is_empty() {
      return Err(anyhow::anyhow!(
        "RDS cluster not found: {}",
        self.cluster_id
      ));
    }

    Ok(response_clusters[0].status().unwrap_or("").to_string())
  }

  async fn is_running(&mut self) -> Result<bool> {
    if let Some(cached_status) = self.cached_running_status {
      return Ok(cached_status);
    }

    let status = self.get_cluster_status().await?;
    let is_available = status == CLUSTER_STATUS_AVAILABLE;

    self.cached_running_status = Some(is_available);
    Ok(is_available)
  }
}
