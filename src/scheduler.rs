use actix_web::{web, App, HttpResponse, HttpServer, Result as ActixResult};
use anyhow::{anyhow, Result};
use calendar_duration::CalendarDuration;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use kube::{
  api::{Api, ListParams, PostParams},
  config::KubeConfigOptions,
  Client, Config,
};
use log::{error, info};

use std::time::Duration as StdDuration;
use tokio::time::interval;

use std::{
  collections::HashMap,
  env,
  sync::{Arc, Mutex},
};
use time::{Duration, OffsetDateTime, UtcDateTime};

use crate::{
  channel::get_data_channel_map_from_env,
  rds::RDSManager,
  record_stream::{get_data_channel_topic_map_from_env, KafkaLagChecker, KafkaRecordStreamFactory},
  slack::SlackClient,
};

const JOB_NAMESPACE_ENV_KEY: &str = "JOB_NAMESPACE";
const DEFAULT_JOB_NAMESPACE: &str = "star-staging";

const JOB_KUBERNETES_CONTEXT_ENV_KEY: &str = "JOB_KUBERNETES_CONTEXT";

const CRONJOB_NAMES_ENV_KEY: &str = "CRONJOB_NAMES";
const DEFAULT_CRONJOB_NAMES: &str = "typical=aggregator-typical";

const SCHEDULER_KAFKA_LAG_THRESHOLDS_ENV_KEY: &str = "SCHEDULER_KAFKA_LAG_THRESHOLDS";
const DEFAULT_SCHEDULER_KAFKA_LAG_THRESHOLDS: &str = "typical=100000000";

const SCHEDULER_MIN_TIME_BETWEEN_JOBS_ENV_KEY: &str = "SCHEDULER_MIN_TIME_BETWEEN_JOBS";
const DEFAULT_SCHEDULER_MIN_TIME_BETWEEN_JOBS: &str = "typical=3d";

const BACKOFF_BASE_DELAY: Duration = Duration::minutes(1);
const BACKOFF_MAX_DELAY: Duration = Duration::hours(12);

const JOB_IMMINENT_THRESHOLD: Duration = Duration::minutes(30);

struct JobState {
  last_job_id: Option<String>,
  retry_count: u32,
  scheduled_at: Option<UtcDateTime>,
  last_job_success_time: Option<UtcDateTime>,
}

struct ChannelInfo {
  kafka_lag_threshold: usize,
  min_time_between_jobs: CalendarDuration,
  cronjob_name: String,
  lag_checker: KafkaLagChecker,
}

pub struct Scheduler {
  kube_client: Client,
  job_states: Arc<Mutex<HashMap<String, JobState>>>,
  job_namespace: String,
  channels: HashMap<String, ChannelInfo>,
  rds_manager: Option<RDSManager>,
  slack_client: Arc<SlackClient>,
}

async fn start_eviction_server(
  job_states: Arc<Mutex<HashMap<String, JobState>>>,
  slack_client: Arc<SlackClient>,
) -> Result<()> {
  info!("Starting eviction notification API server on port 8082");

  HttpServer::new(move || {
    App::new()
      .app_data(web::Data::new(job_states.clone()))
      .app_data(web::Data::new(slack_client.clone()))
      .route("/eviction/{channel}", web::post().to(handle_eviction))
  })
  .bind(("0.0.0.0", 8082))?
  .run()
  .await?;

  Ok(())
}

async fn schedule_job_with_backoff(
  state: &mut JobState,
  channel_name: &str,
  is_eviction: bool,
  slack_client: &SlackClient,
) {
  // Exponential backoff: start at 1 minute, double on each retry, cap at 12 hours
  let delay = (BACKOFF_BASE_DELAY * (2_u32.pow(state.retry_count))).min(BACKOFF_MAX_DELAY);

  state.retry_count += 1;
  state.scheduled_at = Some(UtcDateTime::now() + delay);
  state.last_job_id = None;

  let message = if is_eviction {
    format!(
      "⚠️ Job for channel '{}' received spot eviction. Will run after {}.",
      channel_name,
      delay.to_string()
    )
  } else {
    format!(
      "🚨 Job for channel '{}' failed. Will retry after {}.",
      channel_name,
      delay.to_string()
    )
  };

  info!("{}", message);

  // Send Slack notification
  if let Err(e) = slack_client.send_message(&message).await {
    error!("Failed to send Slack notification: {:?}", e);
  }
}

fn get_job_name_prefix(channel_name: &str) -> String {
  format!("aggregator-{}", channel_name)
}

async fn handle_eviction(
  path: web::Path<String>,
  job_states: web::Data<Arc<Mutex<HashMap<String, JobState>>>>,
  slack_client: web::Data<Arc<SlackClient>>,
) -> ActixResult<HttpResponse> {
  let channel = path.into_inner();

  let mut states = job_states.lock().unwrap();
  let mut resp = if let Some(state) = states.get_mut(&channel) {
    schedule_job_with_backoff(state, &channel, true, slack_client.as_ref()).await;

    HttpResponse::NoContent()
  } else {
    HttpResponse::NotFound()
  };

  Ok(resp.finish())
}

impl Scheduler {
  pub async fn new() -> Result<Self> {
    rustls::crypto::ring::default_provider()
      .install_default()
      .expect("Failed to install default crypto provider");

    let kube_client = if let Ok(context) = env::var(JOB_KUBERNETES_CONTEXT_ENV_KEY) {
      info!("Using Kubernetes context: {}", context);
      let config = Config::from_kubeconfig(&KubeConfigOptions {
        context: Some(context),
        cluster: None,
        user: None,
      })
      .await?;
      Client::try_from(config)?
    } else {
      info!("Using default Kubernetes context");
      Client::try_default().await?
    };

    let topic_map = get_data_channel_topic_map_from_env(false);

    let job_states = topic_map
      .keys()
      .map(|channel_name| {
        (
          channel_name.clone(),
          JobState {
            last_job_id: None,
            retry_count: 0,
            scheduled_at: None,
            last_job_success_time: None,
          },
        )
      })
      .collect();

    let threshold_map = get_data_channel_map_from_env(
      SCHEDULER_KAFKA_LAG_THRESHOLDS_ENV_KEY,
      DEFAULT_SCHEDULER_KAFKA_LAG_THRESHOLDS,
    );

    let cronjob_names = get_data_channel_map_from_env(CRONJOB_NAMES_ENV_KEY, DEFAULT_CRONJOB_NAMES);

    let min_time_map = get_data_channel_map_from_env(
      SCHEDULER_MIN_TIME_BETWEEN_JOBS_ENV_KEY,
      DEFAULT_SCHEDULER_MIN_TIME_BETWEEN_JOBS,
    );

    let factory = KafkaRecordStreamFactory::new();
    let mut channels = HashMap::new();

    for (channel_name, topic_name) in topic_map.iter() {
      let lag_checker = factory.create_lag_checker(false, topic_name.clone())?;

      let threshold_str = threshold_map.get(channel_name).ok_or_else(|| {
        anyhow!(
          "No Kafka lag threshold defined for channel '{}'",
          channel_name
        )
      })?;
      let kafka_lag_threshold = threshold_str.parse::<usize>().map_err(|_| {
        anyhow!(
          "Failed to parse threshold '{}' for channel '{}'",
          threshold_str,
          channel_name
        )
      })?;

      let duration_str = min_time_map.get(channel_name).ok_or_else(|| {
        anyhow!(
          "No min time between jobs defined for channel '{}'",
          channel_name
        )
      })?;
      let min_time_between_jobs = CalendarDuration::from(duration_str.as_str());
      if min_time_between_jobs.is_zero() {
        return Err(anyhow!(
          "Failed to parse min time between jobs for channel {}",
          channel_name
        ));
      }

      let cronjob_name = cronjob_names
        .get(channel_name)
        .ok_or_else(|| anyhow!("No cronjob name defined for channel '{}'", channel_name))?
        .clone();

      let channel_info = ChannelInfo {
        kafka_lag_threshold,
        min_time_between_jobs,
        cronjob_name,
        lag_checker,
      };

      channels.insert(channel_name.clone(), channel_info);
    }

    let rds_manager = RDSManager::load().await;
    info!(
      "RDS management {}",
      if rds_manager.is_some() {
        "enabled"
      } else {
        "disabled"
      }
    );

    let scheduler = Self {
      kube_client,
      job_states: Arc::new(Mutex::new(job_states)),
      job_namespace: env::var(JOB_NAMESPACE_ENV_KEY)
        .unwrap_or_else(|_| DEFAULT_JOB_NAMESPACE.to_string()),
      channels,
      rds_manager,
      slack_client: Arc::new(SlackClient::new()),
    };

    // Populate last_job_id before running scheduler loop
    scheduler.populate_last_job_ids().await?;

    Ok(scheduler)
  }

  async fn populate_last_job_ids(&self) -> Result<()> {
    let jobs_api: Api<Job> = Api::namespaced(self.kube_client.clone(), &self.job_namespace);
    let jobs = jobs_api.list(&ListParams::default()).await?;

    let mut states = self.job_states.lock().unwrap();

    for (channel_name, state) in states.iter_mut() {
      let job_name_prefix = get_job_name_prefix(channel_name);

      for job in &jobs.items {
        if !job
          .metadata
          .name
          .as_deref()
          .unwrap_or_default()
          .starts_with(&job_name_prefix)
        {
          continue;
        }

        if let Some(status) = &job.status {
          if status.active.unwrap_or_default() > 0 {
            let job_name = job.metadata.name.clone().unwrap();
            info!("Found active job {} for channel {}", job_name, channel_name);
            state.last_job_id = Some(job_name);
            break;
          }
        }
      }
    }

    Ok(())
  }

  pub async fn run(mut self) -> Result<()> {
    tokio::select! {
        result = start_eviction_server(self.job_states.clone(), self.slack_client.clone()) => {
            error!("Eviction server exited unexpectedly: {:?}", result);
            result?;
        }
        result = self.scheduler_loop() => {
            error!("Scheduler loop exited unexpectedly: {:?}", result);
            result?;
        }
    }

    Ok(())
  }

  async fn check_and_update_job_statuses(
    &self,
    states: &mut HashMap<String, JobState>,
  ) -> Result<()> {
    if !states.values().any(|state| state.last_job_id.is_some()) {
      return Ok(());
    }

    // Check status of existing jobs
    let jobs: Api<Job> = Api::namespaced(self.kube_client.clone(), &self.job_namespace);
    let all_jobs = jobs.list(&ListParams::default()).await?;
    for (channel_name, state) in states.iter_mut() {
      if let Some(last_job_id) = state.last_job_id.clone() {
        let mut job_failed = false;

        let job = all_jobs
          .items
          .iter()
          .find(|job| job.metadata.name.as_ref() == Some(&last_job_id));

        if let Some(job) = job {
          if let Some(status) = &job.status {
            // Check if job is still active
            if status.active.unwrap_or_default() > 0 {
              continue;
            }
            // Check if job failed
            if status.failed.unwrap_or_default() > 0 {
              job_failed = true;
            }
          }
        }

        state.last_job_id = None;

        // Set success time only if job completed without failures
        if !job_failed {
          state.last_job_success_time = Some(UtcDateTime::now());
          state.retry_count = 0;
          info!(
            "Job {} completed successfully for channel {}",
            last_job_id, channel_name
          );
        } else {
          schedule_job_with_backoff(state, channel_name, false, &self.slack_client).await;
        }
      }
    }
    Ok(())
  }

  async fn check_kafka_lag(&self, channel_name: &str, state: &mut JobState) -> Result<()> {
    if state.scheduled_at.is_some() {
      return Ok(());
    }
    let channel_info = self.channels.get(channel_name).unwrap();

    let total_lag = channel_info
      .lag_checker
      .get_total_lag()
      .await
      .map_err(|e| {
        anyhow!(
          "Failed to get kafka lag for channel {}: {:?}",
          channel_name,
          e
        )
      })?;

    if total_lag >= channel_info.kafka_lag_threshold {
      state.scheduled_at = Some(UtcDateTime::now());
      info!(
        "Kafka lag threshold exceeded for channel {}: {} >= {}",
        channel_name, total_lag, channel_info.kafka_lag_threshold
      );
    }
    Ok(())
  }

  async fn scheduler_loop(&mut self) -> Result<()> {
    let mut scheduler_interval = interval(StdDuration::from_secs(60));

    loop {
      // Collect channels that should run jobs
      let (jobs_active_or_imminent, channels_to_run) = {
        let mut states = self.job_states.lock().unwrap();

        self.check_and_update_job_statuses(&mut states).await?;

        let mut jobs_active_or_imminent = false;
        let mut channels_to_run = Vec::new();

        for (channel_name, state) in states.iter_mut() {
          if state.last_job_id.is_some() {
            jobs_active_or_imminent = true;
            continue;
          }

          self.check_kafka_lag(channel_name, state).await?;

          let now = UtcDateTime::now();

          // Check if enough time has passed since last successful job
          if let Some(last_success) = state.last_job_success_time {
            let min_time = self
              .channels
              .get(channel_name)
              .unwrap()
              .min_time_between_jobs
              .clone();
            let lockout_expiry = (OffsetDateTime::from(last_success) + min_time).to_utc();
            if now < lockout_expiry {
              continue; // Not enough time has passed since last success
            }
          }

          // Check if we should run a job for this channel
          if let Some(scheduled_at) = state.scheduled_at {
            // Check if enough time has passed since job was scheduled
            let time_until_job_start = scheduled_at - now;
            if time_until_job_start < JOB_IMMINENT_THRESHOLD {
              jobs_active_or_imminent = true;
            }
            if time_until_job_start >= Duration::ZERO {
              continue;
            }
          } else {
            continue;
          }

          // Avoid running jobs at midnight
          if now.hour() == 0 {
            continue;
          }

          channels_to_run.push(channel_name.clone());
        }

        (jobs_active_or_imminent, channels_to_run)
      };

      if let Some(rds_manager) = &mut self.rds_manager {
        if jobs_active_or_imminent {
          rds_manager.start().await?;
        } else {
          rds_manager.stop().await?;
        }
      } else {
        debug!(
          "RDS management is disabled, skipping database {}",
          if jobs_active_or_imminent {
            "start"
          } else {
            "stop"
          }
        );
      }

      // Run jobs for each channel
      for channel_name in channels_to_run {
        self.run_job_for_channel(&channel_name).await?;
      }

      // Wait for the next tick (1 minute interval)
      scheduler_interval.tick().await;
    }
  }

  async fn run_job_for_channel(&self, channel_name: &str) -> Result<()> {
    let mut states = self.job_states.lock().unwrap();
    let state = states.get_mut(channel_name).unwrap();

    let jobs: Api<Job> = Api::namespaced(self.kube_client.clone(), &self.job_namespace);
    let cronjobs: Api<CronJob> = Api::namespaced(self.kube_client.clone(), &self.job_namespace);

    // Check if there's already an active job for this channel
    let job_name_prefix = get_job_name_prefix(channel_name);

    // Get the cronjob name for this channel
    let cronjob_name = &self.channels.get(channel_name).unwrap().cronjob_name;

    // Fetch the source CronJob
    let source_cronjob = cronjobs.get(&cronjob_name).await?;

    // Extract the job template from the CronJob
    let job_template = &source_cronjob
      .spec
      .as_ref()
      .ok_or_else(|| anyhow::anyhow!("CronJob {} has no spec", cronjob_name))?
      .job_template;

    let now = UtcDateTime::now();
    let timestamp = format!(
      "{:02}{:02}-{:02}{:02}{:02}",
      now.month() as u8,
      now.day(),
      now.hour(),
      now.minute(),
      now.second()
    );
    let job_name = format!("{}-{}", job_name_prefix, timestamp);

    info!(
      "Starting job {} for channel {} from cronjob {}",
      job_name, channel_name, cronjob_name
    );

    // Create a new Job based on the CronJob template
    let job = Job {
      metadata: kube::api::ObjectMeta {
        name: Some(job_name.clone()),
        namespace: Some(self.job_namespace.clone()),
        ..Default::default()
      },
      spec: job_template.spec.clone(),
      ..Default::default()
    };

    jobs.create(&PostParams::default(), &job).await?;
    info!(
      "Successfully created job {} for channel {} from cronjob {}",
      job_name, channel_name, cronjob_name
    );

    state.last_job_id = Some(job_name.clone());
    state.scheduled_at = None;

    Ok(())
  }
}
