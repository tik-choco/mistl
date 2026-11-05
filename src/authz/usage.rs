use std::collections::HashMap;

use chrono::{DateTime, Datelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::{Denied, Limits, Period, Service};

pub const MAX_ACCOUNTS: usize = 4096;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Meter {
    pub period_start: DateTime<Utc>,
    pub requests: u64,
    pub output_chars: u64,
    pub last_seen: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overflow_period: Option<Period>,
}

impl Default for Meter {
    fn default() -> Self {
        Self {
            period_start: DateTime::UNIX_EPOCH,
            requests: 0,
            output_chars: 0,
            last_seen: DateTime::UNIX_EPOCH,
            overflow_period: None,
        }
    }
}

impl Meter {
    fn rollover_account(&mut self, account: &str, period: Period, now: DateTime<Utc>) {
        if account != "*" {
            self.rollover(period, now);
            return;
        }
        // Many principals share overflow. Switching between their periods
        // must never clear counters; keep the longest period seen and carry
        // existing usage when it grows. Homogeneous overflow rolls as usual.
        let rank = |p| match p {
            Period::Day => 0,
            Period::Month => 1,
            Period::Total => 2,
        };
        match self.overflow_period {
            Some(old) if rank(period) > rank(old) => {
                self.overflow_period = Some(period);
                self.period_start = period_start(period, now);
            }
            Some(old) => self.rollover(old, now),
            None => {
                self.overflow_period = Some(period);
                self.rollover(period, now);
            }
        }
    }

    fn rollover(&mut self, period: Period, now: DateTime<Utc>) {
        let start = period_start(period, now);
        if self.period_start != start {
            self.period_start = start;
            self.requests = 0;
            self.output_chars = 0;
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct UsageBook {
    pub ai: HashMap<String, Meter>,
    pub escrow: HashMap<String, Meter>,
}

impl UsageBook {
    pub fn meters(&self, service: Service) -> &HashMap<String, Meter> {
        match service {
            Service::Ai => &self.ai,
            Service::Escrow => &self.escrow,
        }
    }

    pub fn meters_mut(&mut self, service: Service) -> &mut HashMap<String, Meter> {
        match service {
            Service::Ai => &mut self.ai,
            Service::Escrow => &mut self.escrow,
        }
    }

    pub fn account(&self, service: Service, requested: &str) -> String {
        let meters = self.meters(service);
        if meters.contains_key(requested)
            || meters.len() - usize::from(meters.contains_key("*")) < MAX_ACCOUNTS
        {
            requested.to_owned()
        } else {
            "*".into()
        }
    }

    pub fn check(
        &mut self,
        service: Service,
        account: &str,
        limits: &Limits,
        now: DateTime<Utc>,
    ) -> Result<(), Denied> {
        let meter = self.meters_mut(service).entry(account.into()).or_default();
        meter.rollover_account(account, limits.period, now);
        meter.last_seen = now;
        if limits.max_requests.is_some_and(|max| meter.requests >= max)
            || limits
                .max_output_chars
                .is_some_and(|max| meter.output_chars >= max)
        {
            return Err(Denied::usage_limit());
        }
        Ok(())
    }

    pub fn record(
        &mut self,
        service: Service,
        account: &str,
        limits: &Limits,
        requests: u64,
        output_chars: u64,
        now: DateTime<Utc>,
    ) {
        // An authorization may outlive a reset; resolve the cap again before inserting.
        let account = self.account(service, account);
        let meter = self.meters_mut(service).entry(account.clone()).or_default();
        meter.rollover_account(&account, limits.period, now);
        meter.requests = meter.requests.saturating_add(requests);
        meter.output_chars = meter.output_chars.saturating_add(output_chars);
        meter.last_seen = now;
    }

    pub fn snapshot(
        &self,
        service: Service,
        account: &str,
        limits: &Limits,
        now: DateTime<Utc>,
    ) -> Meter {
        let mut meter = self
            .meters(service)
            .get(account)
            .cloned()
            .unwrap_or_default();
        meter.rollover_account(account, limits.period, now);
        meter
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        for meters in [&self.ai, &self.escrow] {
            anyhow::ensure!(
                meters.len() - usize::from(meters.contains_key("*")) <= MAX_ACCOUNTS,
                "too many usage accounts"
            );
            for account in meters.keys() {
                anyhow::ensure!(
                    account == "*"
                        || account.strip_prefix("node:").is_some_and(|n| {
                            super::policy::validate_subject(n).is_ok() && n.len() == 16
                        })
                        || ["token:", "cap:"].iter().any(|prefix| account
                            .strip_prefix(prefix)
                            .is_some_and(|id| !id.is_empty()
                                && id.len() <= 64
                                && id.bytes().all(|b| b.is_ascii_hexdigit()))),
                    "invalid usage account"
                );
            }
        }
        Ok(())
    }
}

pub fn period_start(period: Period, now: DateTime<Utc>) -> DateTime<Utc> {
    match period {
        Period::Day => Utc
            .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
            .unwrap(),
        Period::Month => Utc
            .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
            .unwrap(),
        Period::Total => DateTime::UNIX_EPOCH,
    }
}
