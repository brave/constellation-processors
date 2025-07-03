use anyhow::Result;
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_rds::Client as RdsClient;
use log::info;
use std::env;

const RDS_MANAGEMENT_ROLE_ENV_KEY: &str = "RDS_MANAGEMENT_ROLE";
const RDS_CLUSTER_ID_ENV_KEY: &str = "RDS_CLUSTER_ID";
const SESSION_NAME: &str = "rds-management-session";

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
    
    self.rds_client
      .start_db_cluster()
      .db_cluster_identifier(&self.cluster_id)
      .send()
      .await?;
    
    self.cached_running_status = Some(true);
    info!("Successfully started RDS cluster: {}", self.cluster_id);
    Ok(())
  }

  pub async fn stop(&mut self) -> Result<()> {
    if !self.is_running().await? {
      return Ok(());
    }

    info!("Stopping RDS cluster: {}", self.cluster_id);
    
    self.rds_client
      .stop_db_cluster()
      .db_cluster_identifier(&self.cluster_id)
      .send()
      .await?;
    
    self.cached_running_status = Some(false);
    info!("Successfully stopped RDS cluster: {}", self.cluster_id);
    Ok(())
  }

  async fn is_running(&mut self) -> Result<bool> {
    if let Some(cached_status) = self.cached_running_status {
      return Ok(cached_status);
    }

    let response = self.rds_client
      .describe_db_clusters()
      .db_cluster_identifier(&self.cluster_id)
      .send()
      .await?;
    
    let response_clusters = response.db_clusters();

    if response_clusters.is_empty() {
        return Ok(false);
    }
    self.cached_running_status = Some(response_clusters[0].status().unwrap() == "available");
    Ok(self.cached_running_status.unwrap())
  }
}