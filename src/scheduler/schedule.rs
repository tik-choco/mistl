//! Schedule expression parsing and next-fire computation. Pure, offline,
//! ported from tc-sched's `normalize.go`. Everything here is deterministic
//! given a `DateTime<Local>` "now" -- no filesystem, no network -- so it's
//! fully covered by the inline unit tests without a daemon.

use std::str::FromStr;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike, Duration, Local, NaiveDate, TimeZone, Weekday};

/// A parsed schedule expression, ready to compute successive fire times
/// from. See `parse` for the accepted syntax.
pub(super) enum Schedule {
    /// `@every Xw <dow> HH:MM`: every X weeks on a given weekday at a fixed
    /// time of day.
    WeeklyOnDow {
        weeks: i64,
        dow: Weekday,
        hour: u32,
        minute: u32,
    },
    /// `@every Xw HH:MM` / `@every Xd HH:MM`: a plain interval snapped to a
    /// fixed time of day.
    IntervalAtTime {
        interval: Duration,
        hour: u32,
        minute: u32,
    },
    /// `@every Xw`, `@every Xd`, `@every XwYd`, or `@every <go-duration>`:
    /// a plain fixed interval from "now".
    Interval(Duration),
    /// A 6-field (with-seconds) cron expression, including the ones mapped
    /// from `@hourly`/`@daily`/etc. Boxed: `cron::Schedule` is much larger
    /// than the interval variants (clippy::large_enum_variant).
    Cron(Box<cron::Schedule>),
}

impl Schedule {
    /// Next fire time strictly after `t`, or `None` if the underlying cron
    /// expression has no more occurrences (the `cron` crate's iterator can
    /// exhaust itself for expressions like an unreachable Feb 30 -- our own
    /// interval variants always return `Some`).
    pub(super) fn next_after(&self, t: DateTime<Local>) -> Option<DateTime<Local>> {
        match self {
            Schedule::WeeklyOnDow {
                weeks,
                dow,
                hour,
                minute,
            } => next_after_weekly_on_dow(t, *weeks, *dow, *hour, *minute),
            Schedule::IntervalAtTime {
                interval,
                hour,
                minute,
            } => next_after_interval_at_time(t, *interval, *hour, *minute),
            Schedule::Interval(interval) => Some(t + *interval),
            Schedule::Cron(schedule) => schedule.after(&t).next(),
        }
    }
}

/// Parses a schedule expression: `@every ...`, a descriptor (`@hourly` etc.),
/// or a 5- or 6-field cron expression. Case-insensitive, whitespace-trimmed.
/// Returns a clear `Err` (surfaced verbatim by `sched.add`/`sched.set`/
/// `sched.next`) on anything that doesn't match one of those forms.
pub(super) fn parse(expr: &str) -> Result<Schedule> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        bail!("empty schedule expression");
    }
    if let Some(rest) = strip_every_prefix(trimmed) {
        return parse_every(rest).with_context(|| format!("invalid @every expression {trimmed:?}"));
    }
    let lower = trimmed.to_ascii_lowercase();
    if let Some(cron_expr) = descriptor_cron(&lower) {
        return parse_cron(cron_expr);
    }
    parse_cron(trimmed)
}

/// Strips a case-insensitive `@every` prefix, requiring a word boundary
/// (whitespace) right after it so `@everything` (say) isn't misread.
fn strip_every_prefix(expr: &str) -> Option<&str> {
    const PREFIX: &str = "@every";
    if expr.len() < PREFIX.len()
        || !expr.is_char_boundary(PREFIX.len())
        || !expr.as_bytes()[..PREFIX.len()].eq_ignore_ascii_case(PREFIX.as_bytes())
    {
        return None;
    }
    let rest = &expr[PREFIX.len()..];
    if rest.is_empty() {
        return Some("");
    }
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    Some(rest.trim())
}

fn descriptor_cron(lower: &str) -> Option<&'static str> {
    match lower {
        "@hourly" => Some("0 0 * * * *"),
        "@daily" | "@midnight" => Some("0 0 0 * * *"),
        "@weekly" => Some("0 0 0 * * SUN"),
        "@monthly" => Some("0 0 0 1 * *"),
        "@yearly" | "@annually" => Some("0 0 0 1 1 *"),
        _ => None,
    }
}

/// Promotes a plain 5-field crontab expression to the 6-field (with-seconds)
/// form the `cron` crate requires, by prepending a `0` seconds field, and
/// rewrites the day-of-week field from crontab convention (0-7, 0/7 = Sunday
/// — what tc-sched's robfig parser accepts) to the crate's Quartz-style 1-7
/// (1 = Sunday), which would otherwise silently shift every numeric weekday
/// by one day.
fn parse_cron(expr: &str) -> Result<Schedule> {
    let mut fields: Vec<String> = expr.split_whitespace().map(str::to_owned).collect();
    if fields.len() == 5 {
        fields.insert(0, "0".to_owned());
    }
    if fields.len() == 6 {
        fields[5] = rewrite_dow_field(&fields[5]);
    }
    let normalized = fields.join(" ");
    let schedule = cron::Schedule::from_str(&normalized).with_context(|| {
        format!("invalid cron expression {expr:?} (normalized to {normalized:?})")
    })?;
    Ok(Schedule::Cron(Box::new(schedule)))
}

/// Maps every numeric weekday in a crontab day-of-week field (0-7, 0/7 =
/// Sunday) to the `cron` crate's 1-7 (1 = Sunday). Handles lists, ranges,
/// and steps; step divisors (after `/`) and non-numeric tokens (`*`, names)
/// pass through untouched. Out-of-range numbers are left alone so the crate
/// reports them as the user wrote them.
fn rewrite_dow_field(field: &str) -> String {
    fn map_num(tok: &str) -> String {
        match tok.parse::<u8>() {
            Ok(n) if n <= 7 => ((n % 7) + 1).to_string(),
            _ => tok.to_owned(),
        }
    }
    fn map_range(range: &str) -> String {
        range.split('-').map(map_num).collect::<Vec<_>>().join("-")
    }
    field
        .split(',')
        .map(|part| match part.split_once('/') {
            Some((range, step)) => format!("{}/{}", map_range(range), step),
            None => map_range(part),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_every(rest: &str) -> Result<Schedule> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    match parts.len() {
        3 => {
            let weeks = parse_weeks_only(parts[0]).with_context(|| {
                format!("expected a week interval like \"2w\", got {:?}", parts[0])
            })?;
            let dow = parse_dow(parts[1])
                .with_context(|| format!("expected a weekday (sun..sat), got {:?}", parts[1]))?;
            let (hour, minute) = parse_hhmm(parts[2])
                .with_context(|| format!("expected a time like \"21:00\", got {:?}", parts[2]))?;
            Ok(Schedule::WeeklyOnDow {
                weeks,
                dow,
                hour,
                minute,
            })
        }
        2 => {
            let (weeks, days) = parse_week_day(parts[0]).with_context(|| {
                format!(
                    "expected an interval like \"2w\" or \"3d\", got {:?}",
                    parts[0]
                )
            })?;
            let (hour, minute) = parse_hhmm(parts[1])
                .with_context(|| format!("expected a time like \"21:00\", got {:?}", parts[1]))?;
            Ok(Schedule::IntervalAtTime {
                interval: Duration::days(weeks * 7 + days),
                hour,
                minute,
            })
        }
        1 => {
            if let Some((weeks, days)) = parse_week_day(parts[0]) {
                return Ok(Schedule::Interval(Duration::days(weeks * 7 + days)));
            }
            if let Some((h, m, s)) = parse_hms(parts[0]) {
                let interval = Duration::hours(h) + Duration::minutes(m) + Duration::seconds(s);
                if interval <= Duration::zero() {
                    bail!("@every duration must be positive, got {:?}", parts[0]);
                }
                return Ok(Schedule::Interval(interval));
            }
            bail!("could not parse @every interval {:?}", parts[0]);
        }
        0 => bail!("@every needs an interval, e.g. \"@every 90m\""),
        _ => bail!("too many fields in @every expression: {rest:?}"),
    }
}

/// Splits a compact unit string like `"2w1d"` or `"1h30m"` into
/// `(number, unit_char)` pairs, in order. `None` if anything doesn't fit the
/// `<digits><letter>` pattern (no separators, no leftover characters).
fn parse_unit_pairs(s: &str) -> Option<Vec<(i64, char)>> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut result = Vec::new();
    let mut chars = s.chars().peekable();
    while chars.peek().is_some() {
        let mut digits = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                digits.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if digits.is_empty() {
            return None;
        }
        let unit = chars.next()?.to_ascii_lowercase();
        let number: i64 = digits.parse().ok()?;
        result.push((number, unit));
    }
    Some(result)
}

fn parse_weeks_only(s: &str) -> Option<i64> {
    let pairs = parse_unit_pairs(s)?;
    if pairs.len() == 1 && pairs[0].1 == 'w' {
        Some(pairs[0].0)
    } else {
        None
    }
}

/// Weeks/days combination for `@every Xw`, `@every Xd`, `@every XwYd`
/// (and the interval half of `@every Xw HH:MM` / `@every Xd HH:MM`). Units
/// outside `{w, d}` (or a repeated unit) are rejected so this doesn't
/// accidentally accept an h/m/s go-duration too.
fn parse_week_day(s: &str) -> Option<(i64, i64)> {
    let pairs = parse_unit_pairs(s)?;
    let mut weeks = 0i64;
    let mut days = 0i64;
    let mut seen_w = false;
    let mut seen_d = false;
    for (n, unit) in pairs {
        match unit {
            'w' if !seen_w => {
                seen_w = true;
                weeks = n;
            }
            'd' if !seen_d => {
                seen_d = true;
                days = n;
            }
            _ => return None,
        }
    }
    Some((weeks, days))
}

/// Go-style duration (`h`/`m`/`s` components, e.g. `"1h30m"`, `"45s"`).
/// Units outside `{h, m, s}` (or a repeated unit) are rejected.
fn parse_hms(s: &str) -> Option<(i64, i64, i64)> {
    let pairs = parse_unit_pairs(s)?;
    let mut hours = 0i64;
    let mut minutes = 0i64;
    let mut seconds = 0i64;
    let mut seen_h = false;
    let mut seen_m = false;
    let mut seen_s = false;
    for (n, unit) in pairs {
        match unit {
            'h' if !seen_h => {
                seen_h = true;
                hours = n;
            }
            'm' if !seen_m => {
                seen_m = true;
                minutes = n;
            }
            's' if !seen_s => {
                seen_s = true;
                seconds = n;
            }
            _ => return None,
        }
    }
    Some((hours, minutes, seconds))
}

fn parse_dow(s: &str) -> Option<Weekday> {
    match s.to_ascii_lowercase().as_str() {
        "sun" => Some(Weekday::Sun),
        "mon" => Some(Weekday::Mon),
        "tue" => Some(Weekday::Tue),
        "wed" => Some(Weekday::Wed),
        "thu" => Some(Weekday::Thu),
        "fri" => Some(Weekday::Fri),
        "sat" => Some(Weekday::Sat),
        _ => None,
    }
}

/// `HH:MM`, 24-hour, zero-padded minutes (hour may be 1 or 2 digits).
fn parse_hhmm(s: &str) -> Option<(u32, u32)> {
    let (h, m) = s.split_once(':')?;
    if h.is_empty() || m.len() != 2 {
        return None;
    }
    if !h.bytes().all(|b| b.is_ascii_digit()) || !m.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hour: u32 = h.parse().ok()?;
    let minute: u32 = m.parse().ok()?;
    if hour > 23 || minute > 59 {
        return None;
    }
    Some((hour, minute))
}

/// `date` at `hour:minute:00` in the local timezone. Ambiguous times (DST
/// fall-back) resolve to the earlier occurrence; a nonexistent time
/// (DST spring-forward) is nudged forward minute by minute (bounded, since
/// the gap is at most a couple of hours in practice) rather than silently
/// dropping the fire.
fn local_at(date: NaiveDate, hour: u32, minute: u32) -> Option<DateTime<Local>> {
    for bump in 0..180 {
        let total_minutes = hour * 60 + minute + bump;
        let (h, m) = (total_minutes / 60, total_minutes % 60);
        if h > 23 {
            break;
        }
        match Local.with_ymd_and_hms(date.year(), date.month(), date.day(), h, m, 0) {
            chrono::LocalResult::Single(dt) => return Some(dt),
            chrono::LocalResult::Ambiguous(dt, _) => return Some(dt),
            chrono::LocalResult::None => continue,
        }
    }
    None
}

/// Port of tc-sched's next-time algorithm for `@every Xw <dow> HH:MM`:
/// `min_time = t + X*7 days`; candidate = that date at `HH:MM`; advance to
/// the target weekday; if candidate is still `<= t`, add another week.
fn next_after_weekly_on_dow(
    t: DateTime<Local>,
    weeks: i64,
    dow: Weekday,
    hour: u32,
    minute: u32,
) -> Option<DateTime<Local>> {
    let min_time = t + Duration::days(weeks * 7);
    let mut candidate = local_at(min_time.date_naive(), hour, minute)?;
    let advance = (dow.num_days_from_monday() as i64
        - candidate.weekday().num_days_from_monday() as i64)
        .rem_euclid(7);
    candidate += Duration::days(advance);
    if candidate <= t {
        candidate += Duration::days(7);
    }
    Some(candidate)
}

/// Port of tc-sched's next-time algorithm for `@every Xw HH:MM` /
/// `@every Xd HH:MM`: `base = t + interval`; `next` = base's date at
/// `HH:MM`; if `next <= t`, add 24h.
fn next_after_interval_at_time(
    t: DateTime<Local>,
    interval: Duration,
    hour: u32,
    minute: u32,
) -> Option<DateTime<Local>> {
    let base = t + interval;
    let mut next = local_at(base.date_naive(), hour, minute)?;
    if next <= t {
        next += Duration::hours(24);
    }
    Some(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    fn local(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Local> {
        Local
            .with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .expect("unambiguous test time")
    }

    #[test]
    fn every_weekly_on_dow_advances_to_target_weekday() {
        // 2026-07-10 is a Friday. Every 2 weeks on Sunday at 21:00.
        let sched = parse("@every 2w Sun 21:00").unwrap();
        let t = local(2026, 7, 10, 8, 0);
        let next = sched.next_after(t).unwrap();
        assert_eq!(next.weekday(), Weekday::Sun);
        // min_time = t + 14 days = 2026-07-24 (Fri); advance to next Sunday = 2026-07-26.
        assert_eq!(next, local(2026, 7, 26, 21, 0));
    }

    #[test]
    fn every_weekly_on_dow_is_case_insensitive() {
        let sched = parse("@EVERY 1w sun 06:00").unwrap();
        // 2026-07-10 is Friday; min_time = t + 7 days = 2026-07-17 (also Friday);
        // advance 2 days to Sunday 2026-07-19, which is still > t, so no extra week.
        let t = local(2026, 7, 10, 8, 0);
        let next = sched.next_after(t).unwrap();
        assert_eq!(next, local(2026, 7, 19, 6, 0));
    }

    #[test]
    fn every_weekly_on_dow_adds_a_week_when_the_candidate_time_already_passed() {
        let sched = parse("@every 0w Sun 06:00").unwrap();
        // 2026-07-12 is a Sunday; min_time = t (weeks = 0), landing the
        // candidate on today at 06:00 -- already before t's 08:00, so it
        // rolls to the following Sunday instead of firing "in the past".
        let t = local(2026, 7, 12, 8, 0);
        assert_eq!(sched.next_after(t).unwrap(), local(2026, 7, 19, 6, 0));
    }

    #[test]
    fn every_interval_at_time_snaps_to_time_of_day() {
        let sched = parse("@every 1d 09:30").unwrap();
        let t = local(2026, 7, 10, 8, 0);
        // base = t + 1 day = 2026-07-11 08:00; that date at 09:30 is already > t.
        assert_eq!(sched.next_after(t).unwrap(), local(2026, 7, 11, 9, 30));

        // A zero-length interval just snaps to the next HH:MM, rolling to
        // the next day once that time has already passed relative to t
        // (exercises the "if next <= t, add 24h" branch).
        let same_day = parse("@every 0d 09:30").unwrap();
        let before = local(2026, 7, 10, 8, 0);
        assert_eq!(
            same_day.next_after(before).unwrap(),
            local(2026, 7, 10, 9, 30)
        );
        let after = local(2026, 7, 10, 23, 0);
        assert_eq!(
            same_day.next_after(after).unwrap(),
            local(2026, 7, 11, 9, 30)
        );
    }

    #[test]
    fn every_plain_week_day_interval() {
        let t = local(2026, 7, 10, 8, 0);
        assert_eq!(
            parse("@every 2w").unwrap().next_after(t).unwrap(),
            t + Duration::days(14)
        );
        assert_eq!(
            parse("@every 3d").unwrap().next_after(t).unwrap(),
            t + Duration::days(3)
        );
        assert_eq!(
            parse("@every 1w2d").unwrap().next_after(t).unwrap(),
            t + Duration::days(9)
        );
    }

    #[test]
    fn every_go_duration_interval() {
        let t = local(2026, 7, 10, 8, 0);
        assert_eq!(
            parse("@every 90m").unwrap().next_after(t).unwrap(),
            t + Duration::minutes(90)
        );
        assert_eq!(
            parse("@every 1h30m").unwrap().next_after(t).unwrap(),
            t + Duration::hours(1) + Duration::minutes(30)
        );
        assert_eq!(
            parse("@every 45s").unwrap().next_after(t).unwrap(),
            t + Duration::seconds(45)
        );
    }

    #[test]
    fn every_zero_or_negative_duration_is_rejected() {
        assert!(parse("@every 0s").is_err());
    }

    #[test]
    fn descriptors_map_to_expected_cron_semantics() {
        // 2026-07-10 is a Friday.
        let t = local(2026, 7, 10, 12, 0);
        // @daily should fire at the next midnight.
        let next = parse("@daily").unwrap().next_after(t).unwrap();
        assert_eq!(next, local(2026, 7, 11, 0, 0));
        // @hourly fires at the next top of the hour.
        let next = parse("@hourly").unwrap().next_after(t).unwrap();
        assert_eq!(next, local(2026, 7, 10, 13, 0));
        // @weekly fires on Sunday at midnight.
        let next = parse("@weekly").unwrap().next_after(t).unwrap();
        assert_eq!(next.weekday(), Weekday::Sun);
        assert_eq!((next.hour(), next.minute()), (0, 0));
        // @midnight is an alias for @daily.
        assert_eq!(
            parse("@midnight").unwrap().next_after(t).unwrap(),
            local(2026, 7, 11, 0, 0)
        );
        // @monthly / @yearly / @annually just need to parse and produce a plausible time.
        assert!(parse("@monthly").unwrap().next_after(t).is_some());
        assert!(parse("@yearly").unwrap().next_after(t).is_some());
        assert!(parse("@annually").unwrap().next_after(t).is_some());
    }

    #[test]
    fn five_field_cron_is_promoted_to_six_fields() {
        // "0 9 * * *" (5-field, every day at 09:00) should behave the same
        // as its 6-field equivalent with an explicit seconds field.
        let t = local(2026, 7, 10, 8, 0);
        let five = parse("0 9 * * *").unwrap();
        let six = parse("0 0 9 * * *").unwrap();
        assert_eq!(five.next_after(t), six.next_after(t));
    }

    #[test]
    fn crontab_day_of_week_numbers_mean_crontab_days() {
        // 2026-07-10 is a Friday. In crontab (and tc-sched's robfig parser),
        // day-of-week 1 = Monday and 0 = Sunday; the `cron` crate's native
        // convention is 1 = Sunday, so without the rewrite these would all
        // land one day early.
        let t = local(2026, 7, 10, 12, 0);
        let monday = parse("0 0 * * 1").unwrap().next_after(t).unwrap();
        assert_eq!(monday.weekday(), Weekday::Mon);
        let sunday = parse("0 0 * * 0").unwrap().next_after(t).unwrap();
        assert_eq!(sunday.weekday(), Weekday::Sun);
        // Vixie cron also accepts 7 for Sunday.
        let sunday7 = parse("0 0 * * 7").unwrap().next_after(t).unwrap();
        assert_eq!(sunday7.weekday(), Weekday::Sun);
        // Names are shared by both conventions and pass through untouched.
        let named = parse("0 0 * * MON").unwrap().next_after(t).unwrap();
        assert_eq!(named.weekday(), Weekday::Mon);
    }

    #[test]
    fn rewrite_dow_field_handles_lists_ranges_and_steps() {
        assert_eq!(rewrite_dow_field("*"), "*");
        assert_eq!(rewrite_dow_field("0"), "1");
        assert_eq!(rewrite_dow_field("6"), "7");
        assert_eq!(rewrite_dow_field("7"), "1");
        assert_eq!(rewrite_dow_field("1-5"), "2-6"); // Mon-Fri
        assert_eq!(rewrite_dow_field("1,3,5"), "2,4,6");
        assert_eq!(rewrite_dow_field("1-5/2"), "2-6/2"); // step divisor untouched
        assert_eq!(rewrite_dow_field("*/2"), "*/2");
        assert_eq!(rewrite_dow_field("MON-FRI"), "MON-FRI");
        assert_eq!(rewrite_dow_field("9"), "9"); // out of range: crate reports it
    }

    #[test]
    fn raw_six_field_cron_expression_parses() {
        let sched = parse("0 0 9 * * *").unwrap();
        let t = local(2026, 7, 10, 8, 0);
        assert_eq!(sched.next_after(t).unwrap(), local(2026, 7, 10, 9, 0));
    }

    #[test]
    fn invalid_expressions_are_rejected_with_context() {
        assert!(parse("").is_err());
        assert!(parse("   ").is_err());
        assert!(parse("@every").is_err());
        assert!(parse("@every notaduration").is_err());
        assert!(parse("@every 2w notaday 21:00").is_err());
        assert!(parse("@every 2w Sun 25:99").is_err());
        assert!(parse("not a cron expression at all").is_err());
        assert!(parse("* * * * * * * *").is_err());
    }
}
