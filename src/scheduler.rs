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
use rand::Rng;
use std::time::Duration as StdDuration;

use std::{
  collections::HashMap,
  env,
  sync::{Arc, Mutex},
};
use time::{Duration, OffsetDateTime};

use crate::{
  channel::get_data_channel_map_from_env,
  record_stream::{
    get_data_channel_topic_map_from_env, KafkaRecordStream, KafkaRecordStreamConfig,
    KafkaRecordStreamFactory,
  },
  rds::RDSManager,
};

const JOB_NAMESPACE_ENV_KEY: &str = "JOB_NAMESPACE";
const DEFAULT_JOB_NAMESPACE: &str = "star-staging";

const JOB_KUBERNETES_CONTEXT_ENV_KEY: &str = "JOB_KUBERNETES_CONTEXT";

const CRONJOB_NAMES_ENV_KEY: &str = "CRONJOB_NAMES";
const DEFAULT_CRONJOB_NAMES: &str = "typical=aggregator-typical";

const SCHEDULER_KAFKA_LAG_THRESHOLDS_ENV_KEY: &str = "SCHEDULER_KAFKA_LAG_THRESHOLDS";
const DEFAULT_SCHEDULER_KAFKA_LAG_THRESHOLDS: &str = "typical=100000000";

const SCHEDULER_MAX_TIME_BETWEEN_JOBS_ENV_KEY: &str = "SCHEDULER_MAX_TIME_BETWEEN_JOBS";
const DEFAULT_SCHEDULER_MAX_TIME_BETWEEN_JOBS: &str = "typical=3d";

struct JobState {
  last_job_id: Option<String>,
  schedule_after_kafka_lag_duration: Duration,
  kafka_lag_threshold_set_at: Option<OffsetDateTime>,
  last_job_success_time: Option<OffsetDateTime>,
}

pub struct Scheduler {
  kube_client: Client,
  job_states: Arc<Mutex<HashMap<String, JobState>>>,
  job_namespace: String,
  record_streams: HashMap<String, KafkaRecordStream>,
  kafka_lag_thresholds: HashMap<String, usize>,
  cronjob_names: HashMap<String, String>,
  max_time_between_jobs: HashMap<String, CalendarDuration>,
  rds_manager: Option<RDSManager>,
}

async fn start_eviction_server(job_states: Arc<Mutex<HashMap<String, JobState>>>) -> Result<()> {
  info!("Starting eviction notification API server on port 8082");

  HttpServer::new(move || {
    let job_states = job_states.clone();
    App::new()
      .app_data(web::Data::new(job_states))
      .route("/eviction/{channel}", web::post().to(handle_eviction))
  })
  .bind(("0.0.0.0", 8082))?
  .run()
  .await?;

  Ok(())
}

fn generate_schedule_after_kafka_lag_duration() -> Duration {
  let mut rng = rand::thread_rng();
  let random_hours = rng.gen_range(1..=12);
  Duration::hours(random_hours)
}

async fn handle_eviction(
  path: web::Path<String>,
  job_states: web::Data<Arc<Mutex<HashMap<String, JobState>>>>,
) -> ActixResult<HttpResponse> {
  let channel = path.into_inner();

  let mut states = job_states.lock().unwrap();
  if let Some(state) = states.get_mut(&channel) {
    // Set random duration between 3 and 12 hours
    state.schedule_after_kafka_lag_duration = generate_schedule_after_kafka_lag_duration();
    state.kafka_lag_threshold_set_at = Some(OffsetDateTime::now_utc());
    state.last_job_id = None;

    info!(
      "Channel {} evicted, will run after {} hours from now",
      channel, state.schedule_after_kafka_lag_duration.whole_hours()
    );

    Ok(HttpResponse::NoContent().finish())
  } else {
    Ok(HttpResponse::NotFound().finish())
  }
}

impl Scheduler {
  pub async fn new() -> Result<Self> {
    rustls::crypto::ring::default_provider().install_default().expect("Failed to install default crypto provider");

    let kube_client = if let Ok(context) = env::var(JOB_KUBERNETES_CONTEXT_ENV_KEY) {
      info!("Using Kubernetes context: {}", context);
      let config = Config::from_kubeconfig(&KubeConfigOptions {
        context: Some(context),
        cluster: None,
        user: None,
      }).await?;
      Client::try_from(config)?
    } else {
      info!("Using default Kubernetes context");
      Client::try_default().await?
    };

    let topic_map = get_data_channel_topic_map_from_env(false);

    let mut job_states = HashMap::new();

    for channel_name in topic_map.keys() {
      job_states.insert(
        channel_name.clone(),
        JobState {
          last_job_id: None,
          schedule_after_kafka_lag_duration: Duration::ZERO,
          kafka_lag_threshold_set_at: None,
          last_job_success_time: None,
        },
      );
    }

    // Parse kafka lag thresholds
    let threshold_map = get_data_channel_map_from_env(
      SCHEDULER_KAFKA_LAG_THRESHOLDS_ENV_KEY,
      DEFAULT_SCHEDULER_KAFKA_LAG_THRESHOLDS,
    );
    
    let kafka_lag_thresholds: HashMap<String, usize> = threshold_map
      .into_iter()
      .map(|(channel_name, threshold_str)| {
        threshold_str.parse::<usize>()
          .map_err(|_| anyhow!("Failed to parse threshold '{}' for channel '{}'", threshold_str, channel_name))
          .map(|threshold| (channel_name, threshold))
      })
      .collect::<Result<HashMap<_, _>, _>>()?;
    
    // Parse cronjob names
    let cronjob_names = get_data_channel_map_from_env(
      CRONJOB_NAMES_ENV_KEY,
      DEFAULT_CRONJOB_NAMES,
    );

    // Parse max time between jobs
    let max_time_map = get_data_channel_map_from_env(
      SCHEDULER_MAX_TIME_BETWEEN_JOBS_ENV_KEY,
      DEFAULT_SCHEDULER_MAX_TIME_BETWEEN_JOBS,
    );
    
    let max_time_between_jobs: HashMap<String, CalendarDuration> = max_time_map
      .into_iter()
      .map(|(channel_name, duration_str)| {
        let duration = CalendarDuration::from(duration_str.as_str());
        if duration.is_zero() {
          return Err(anyhow!("Failed to parse max time between jobs for channel {}", channel_name));
        }
        Ok((channel_name, duration))
      })
      .collect::<Result<HashMap<_, _>, _>>()?;

    // Verify all channels have thresholds, cronjobs, and max times defined
    for channel_name in topic_map.keys() {
      if !kafka_lag_thresholds.contains_key(channel_name) {
        return Err(anyhow!("No Kafka lag threshold defined for channel '{}'", channel_name));
      }
      if !cronjob_names.contains_key(channel_name) {
        return Err(anyhow!("No cronjob name defined for channel '{}'", channel_name));
      }
      if !max_time_between_jobs.contains_key(channel_name) {
        return Err(anyhow!("No max time between jobs defined for channel '{}'", channel_name));
      }
    }

    let factory = KafkaRecordStreamFactory::new();
    let mut record_streams = HashMap::new();

    for (channel_name, topic_name) in topic_map.iter() {
      let stream_config = KafkaRecordStreamConfig {
        enable_producer: false,
        enable_consumer: true,
        topic: topic_name.clone(),
        use_output_group_id: false,
      };
      let record_stream = factory.create_record_stream(stream_config);
      record_streams.insert(channel_name.clone(), record_stream);
    }

    let rds_manager = RDSManager::load().await;
    info!("RDS management {}", if rds_manager.is_some() { "enabled" } else { "disabled" });

    let scheduler = Self {
      kube_client,
      job_states: Arc::new(Mutex::new(job_states)),
      job_namespace: env::var(JOB_NAMESPACE_ENV_KEY)
        .unwrap_or_else(|_| DEFAULT_JOB_NAMESPACE.to_string()),
      record_streams,
      kafka_lag_thresholds,
      cronjob_names,
      max_time_between_jobs,
      rds_manager,
    };

    // Populate last_job_id before running scheduler loop
    scheduler.populate_last_job_ids().await?;

    Ok(scheduler)
  }

  async fn populate_last_job_ids(&self) -> Result<()> {
    let jobs: Api<Job> = Api::namespaced(self.kube_client.clone(), &self.job_namespace);
    let active_jobs = jobs.list(&ListParams::default()).await?;

    let mut states = self.job_states.lock().unwrap();

    for (channel_name, state) in states.iter_mut() {
      let job_name_prefix = format!("aggregator-{}", channel_name);

      for job in &active_jobs.items {
        if let Some(job_name) = &job.metadata.name {
          if !job_name.starts_with(&job_name_prefix) {
            continue;
          }

          if let Some(status) = &job.status {
            if status.active.unwrap_or_default() > 0 {
              state.last_job_id = Some(job_name.clone());
              break;
            }
          }
        }
      }
    }

    Ok(())
  }

  pub async fn run(mut self) -> Result<()> {
    tokio::select! {
        result = start_eviction_server(self.job_states.clone()) => {
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
    // Check status of existing jobs
    let jobs: Api<Job> = Api::namespaced(self.kube_client.clone(), &self.job_namespace);
    let all_jobs = jobs.list(&ListParams::default()).await?;
    for (channel_name, state) in states.iter_mut() {
      if let Some(last_job_id) = state.last_job_id.clone() {
        let mut job_failed = false;

        let job = all_jobs.items.iter().find(|job| {
          job.metadata.name.as_ref() == Some(&last_job_id)
        });

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
          state.last_job_success_time = Some(OffsetDateTime::now_utc());
          info!("Job {} completed successfully for channel {}", last_job_id, channel_name);
        } else {
          state.schedule_after_kafka_lag_duration = generate_schedule_after_kafka_lag_duration();
          state.kafka_lag_threshold_set_at = Some(OffsetDateTime::now_utc());

          info!(
            "Job {} failed for channel {}, will retry after {} hours",
            last_job_id, channel_name, state.schedule_after_kafka_lag_duration.whole_hours()
          );
        }
      }
    }
    Ok(())
  }

  async fn check_kafka_lag(&self, channel_name: &str, state: &mut JobState) -> Result<()> {
    if state.kafka_lag_threshold_set_at.is_some() {
      return Ok(());
    }
    let record_stream = self.record_streams.get(channel_name).unwrap();
    let threshold = self.kafka_lag_thresholds.get(channel_name).unwrap();

    let total_lag = record_stream.get_total_kafka_lag().await.map_err(|e| anyhow!("Failed to get kafka lag for channel {}: {:?}", channel_name, e))?;

    if total_lag >= *threshold {
      state.kafka_lag_threshold_set_at = Some(OffsetDateTime::now_utc());
      info!(
        "Kafka lag threshold exceeded for channel {}: {} >= {}",
        channel_name, total_lag, threshold
      );
    }
    Ok(())
  }

  async fn scheduler_loop(&mut self) -> Result<()> {
    loop {
      // Sleep for 1 minute
      tokio::time::sleep(StdDuration::from_secs(60)).await;

      let now = OffsetDateTime::now_utc();

      // Collect channels that should run jobs
      let channels_to_run = {
        let mut states = self.job_states.lock().unwrap();

        self.check_and_update_job_statuses(&mut states).await?;

        let mut channels_to_run = Vec::new();

        for (channel_name, state) in states.iter_mut() {
          if state.last_job_id.is_some() {
            continue;
          }

          // Check if enough time has passed since last successful job
          if let Some(last_success) = state.last_job_success_time {
            let max_time = self.max_time_between_jobs.get(channel_name).unwrap().clone();
            let lockout_expiry = last_success + max_time;
            if now < lockout_expiry {
              continue; // Not enough time has passed since last success
            }
          }

          self.check_kafka_lag(channel_name, state).await?;

          // Check if we should run a job for this channel
          let should_run = if let Some(kafka_lag_set_at) = state.kafka_lag_threshold_set_at {
            // Check if enough time has passed since kafka lag threshold was set
            now - kafka_lag_set_at >= state.schedule_after_kafka_lag_duration
          } else {
            false
          };

          if should_run && now.hour() != 0 {
            // Avoid running jobs at midnight
            channels_to_run.push(channel_name.clone());
          }
        }

        channels_to_run
      };

      if channels_to_run.is_empty() {
        if let Some(rds_manager) = &mut self.rds_manager {
          rds_manager.stop().await?;
        } else {
          debug!("RDS management is disabled, skipping database stop");
        }
        continue;
      }

      if let Some(rds_manager) = &mut self.rds_manager {
        rds_manager.start().await?;
      } else {
        info!("RDS management is disabled, skipping database start");
      }

      // Run jobs for each channel
      for channel_name in channels_to_run {
        self.run_job_for_channel(&channel_name).await?;
      }
    }
  }

  async fn run_job_for_channel(&self, channel_name: &str) -> Result<()> {
    let mut states = self.job_states.lock().unwrap();
    let state = states.get_mut(channel_name).unwrap();

    let jobs: Api<Job> = Api::namespaced(self.kube_client.clone(), &self.job_namespace);
    let cronjobs: Api<CronJob> = Api::namespaced(self.kube_client.clone(), &self.job_namespace);

    // Check if there's already an active job for this channel
    let job_name_prefix = format!("aggregator-{}", channel_name);

    // Get the cronjob name for this channel
    let cronjob_name = self.cronjob_names.get(channel_name).unwrap();

    // Fetch the source CronJob
    let source_cronjob = cronjobs.get(&cronjob_name).await?;

    // Extract the job template from the CronJob
    let job_template = &source_cronjob
      .spec
      .as_ref()
      .ok_or_else(|| anyhow::anyhow!("CronJob {} has no spec", cronjob_name))?
      .job_template;

    let now = OffsetDateTime::now_utc();
    let timestamp = format!(
      "{:02}{:02}-{:02}{:02}{:02}",
      now.month(),
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
    state.kafka_lag_threshold_set_at = None;
    state.schedule_after_kafka_lag_duration = Duration::ZERO;

    Ok(())
  }
}
