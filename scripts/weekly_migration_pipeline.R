#!/usr/bin/env Rscript

# ============================================================
# Weekly Mallard Migration Pipeline (Movebank -> State/Province -> Narrative)
# Outputs:
#   docs/data/state_province_proportions.csv
#   docs/data/device_state_province.csv
#   docs/data/device_migration_metrics.csv
#   docs/data/weekly_summary.json
#   docs/weekly_status.md
#
# Requirements:
#   - MOVEBANK_USER, MOVEBANK_PASS (or MOVEBANK_PASSWORD) in env
#   - R packages: move, dplyr, lubridate, sf, rnaturalearth, jsonlite, readr, stringr
# ============================================================

suppressPackageStartupMessages({
  library(move)
  library(dplyr)
  library(lubridate)
  library(sf)
  library(rnaturalearth)
  library(jsonlite)
  library(readr)
  library(stringr)
})

# ----------------------------
# 0) Parameters
# ----------------------------
study_id <- as.numeric(Sys.getenv("MOVEBANK_STUDY_ID", unset = "2665435998"))

# time window: last 7 full days ending "now" (UTC)
now_utc <- with_tz(Sys.time(), "UTC")
start_utc <- now_utc - days(7)

# Outputs (GitHub Pages uses /docs)
out_dir_data <- Sys.getenv("OUT_DIR_DATA", unset = "docs/data")
out_md <- Sys.getenv("OUT_MD", unset = "docs/weekly_status.md")

dir.create(out_dir_data, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_md), recursive = TRUE, showWarnings = FALSE)

# Migration classification thresholds (tune as desired)
THRESH_NET_KM_MIGRATING     <- as.numeric(Sys.getenv("THRESH_NET_KM_MIGRATING", unset = "150"))
THRESH_LAT_CHANGE_DEG       <- as.numeric(Sys.getenv("THRESH_LAT_CHANGE_DEG", unset = "1.0"))
THRESH_NET_KM_TRANSITIONAL  <- as.numeric(Sys.getenv("THRESH_NET_KM_TRANSITIONAL", unset = "60"))

# ----------------------------
# 1) Movebank login + pull data
# ----------------------------
user <- Sys.getenv("MOVEBANK_USER", unset = "")
pass <- Sys.getenv("MOVEBANK_PASS", unset = Sys.getenv("MOVEBANK_PASSWORD", unset = ""))

if (user == "" || pass == "") {
  stop("Missing Movebank credentials. Set MOVEBANK_USER and MOVEBANK_PASS (or MOVEBANK_PASSWORD).")
}

login <- move::movebankLogin(username = user, password = pass)

mv <- move::getMovebankData(
  study                      = study_id,
  login                      = login,
  sensorID                   = "GPS",
  timestamp_start            = start_utc,
  removeDuplicatedTimestamps = TRUE,
  underscoreToDots           = FALSE
)

df <- as.data.frame(mv)
colnames(df)
# Normalize key columns across Movebank exports
# Common fields: location.long, location.lat, timestamp, individual.local.identifier, tag.local.identifier
# Your file may have Longitude/Latitude; this script handles Movebank defaults.
col_map <- list(
  lon = c("location_long", "longitude", "Longitude", "lon", "LONGITUDE"),
  lat = c("location_lat",  "latitude",  "Latitude",  "lat", "LATITUDE"),
  ts  = c("timestamp", "Timestamp", "time", "UTC_datetime"),
  tag = c("tag_local_identifier"),
  ind = c("individual.local.identifier", "individual_id", "individual", "animal_id")
)

pick_col <- function(d, candidates) {
  hit <- candidates[candidates %in% names(d)]
  if (length(hit) == 0) return(NA_character_)
  hit[[1]]
}

lon_col <- pick_col(df, col_map$lon)
lat_col <- pick_col(df, col_map$lat)
ts_col  <- pick_col(df, col_map$ts)
tag_col <- pick_col(df, col_map$tag)
ind_col <- pick_col(df, col_map$ind)

if (is.na(lon_col) || is.na(lat_col) || is.na(ts_col)) {
  stop("Could not find lon/lat/timestamp columns in Movebank data.")
}

if (is.na(tag_col)) {
  # fall back: use individual id if tag id missing
  tag_col <- ind_col
}
if (is.na(tag_col)) stop("Could not find a transmitter identifier column (tag/local identifier).")

gps <- df %>%
  transmute(
    device_id = as.character(.data[[tag_col]]),
    individual_id = if (!is.na(ind_col)) as.character(.data[[ind_col]]) else NA_character_,
    timestamp = as.POSIXct(.data[[ts_col]], tz = "UTC"),
    lon = as.numeric(.data[[lon_col]]),
    lat = as.numeric(.data[[lat_col]])
  ) %>%
  filter(!is.na(timestamp), !is.na(lon), !is.na(lat)) %>%
  arrange(device_id, timestamp)

if (nrow(gps) == 0) stop("No GPS records returned for the last 7 days.")

# ----------------------------
# 2) Assign state/province via spatial join
# ----------------------------
pts <- st_as_sf(gps, coords = c("lon", "lat"), crs = 4326, remove = FALSE)

# US + Canada admin1 polygons (states/provinces)
admin1 <- rnaturalearth::ne_states(
  country = c("United States of America", "Canada"),
  returnclass = "sf"
)

# keep only relevant fields; 'name' is state/province; 'postal' exists for many US states
admin1 <- admin1 %>%
  select(name, postal, iso_a2, geonunit, iso_3166_2)

# spatial join
pts_admin <- st_join(pts, admin1, left = TRUE, largest = TRUE)

gps_admin <- pts_admin %>%
  st_drop_geometry() %>%
  mutate(
    state_province = ifelse(!is.na(name), name, "Unknown"),
    state_abbrev   = ifelse(!is.na(postal), postal, NA_character_)
  )

# ----------------------------
# 3) Proportions by state/province (overall + per device)
# ----------------------------
overall_props <- gps_admin %>%
  count(state_province, sort = TRUE, name = "n_fixes") %>%
  mutate(prop = n_fixes / sum(n_fixes)) %>%
  mutate(window_start_utc = start_utc, window_end_utc = now_utc)

device_props <- gps_admin %>%
  count(device_id, state_province, name = "n_fixes") %>%
  group_by(device_id) %>%
  mutate(prop = n_fixes / sum(n_fixes)) %>%
  ungroup() %>%
  arrange(device_id, desc(n_fixes))

# ----------------------------
# 4) Migration metrics per device
# ----------------------------
# Helper: haversine distance (km)
haversine_km <- function(lon1, lat1, lon2, lat2) {
  to_rad <- function(x) x * pi/180
  R <- 6371.0088
  dlat <- to_rad(lat2 - lat1)
  dlon <- to_rad(lon2 - lon1)
  a <- sin(dlat/2)^2 + cos(to_rad(lat1)) * cos(to_rad(lat2)) * sin(dlon/2)^2
  2 * R * asin(pmin(1, sqrt(a)))
}

dev_metrics <- gps_admin %>%
  group_by(device_id) %>%
  summarise(
    n_fixes = n(),
    start_time = min(timestamp, na.rm = TRUE),
    end_time   = max(timestamp, na.rm = TRUE),
    start_lon  = lon[which.min(timestamp)],
    start_lat  = lat[which.min(timestamp)],
    end_lon    = lon[which.max(timestamp)],
    end_lat    = lat[which.max(timestamp)],
    net_km     = haversine_km(start_lon, start_lat, end_lon, end_lat),
    lat_change = end_lat - start_lat,
    max_lat    = max(lat, na.rm = TRUE),
    min_lat    = min(lat, na.rm = TRUE),
    top_state  = {
      tab <- table(state_province)
      names(tab)[which.max(tab)]
    },
    top_state_prop = {
      tab <- table(state_province)
      as.numeric(max(tab) / sum(tab))
    },
    .groups = "drop"
  ) %>%
  mutate(
    migration_status = case_when(
      net_km >= THRESH_NET_KM_MIGRATING & lat_change >= THRESH_LAT_CHANGE_DEG ~ "Migrating",
      net_km >= THRESH_NET_KM_TRANSITIONAL ~ "Transitional",
      TRUE ~ "Local"
    )
  ) %>%
  arrange(desc(net_km))

# ----------------------------
# 5) Narrative generator
# ----------------------------
fmt_pct <- function(x) paste0(round(100 * x, 1), "%")
fmt_km  <- function(x) format(round(x), big.mark = ",")

n_devices <- n_distinct(gps_admin$device_id)
n_fixes   <- nrow(gps_admin)

status_counts <- dev_metrics %>%
  count(migration_status) %>%
  mutate(prop = n / sum(n)) %>%
  arrange(match(migration_status, c("Local","Transitional","Migrating")))

top_states <- overall_props %>% slice_head(n = 6)

# "Leading edge" = top 3 net distance devices
leaders <- dev_metrics %>% slice_head(n = min(3, nrow(dev_metrics)))

window_str <- paste0(
  format(start_utc, "%Y-%m-%d"), " to ", format(now_utc, "%Y-%m-%d"),
  " (UTC)"
)

# Compose markdown narrative
md <- c(
  "# Weekly Mallard Migration Status",
  "",
  paste0("**Window:** ", window_str),
  paste0("**Transmitters reporting:** ", n_devices, "  |  **GPS fixes:** ", format(n_fixes, big.mark = ",")),
  "",
  "## State/Province distribution (all fixes)",
  "",
  paste0(
    "- ", paste0(top_states$state_province, ": ", fmt_pct(top_states$prop)), collapse = "\n- "
  ),
  "",
  "## Migration status (by transmitter)",
  "",
  paste0(
    "- ", paste0(status_counts$migration_status, ": ", status_counts$n, " (", fmt_pct(status_counts$prop), ")"),
    collapse = "\n"
  ),
  "",
  "## Notes",
  "",
  paste0(
    "Most transmitters continue to show **local late-winter movements** within their primary wintering areas. ",
    "A subset of birds are showing **transitional behavior** (larger net displacement without strong latitudinal gain), ",
    "and the **leading edge of spring migration** is evident among birds with substantial northward movement over the last week."
  ),
  "",
  "## Leading movement this week (net displacement)",
  "",
  paste0(
    "- ", leaders$device_id,
    ": **", fmt_km(leaders$net_km), " km** net, lat change **", round(leaders$lat_change, 2), "°**, top admin area **", leaders$top_state, "**",
    collapse = "\n"
  ),
  ""
)

# ----------------------------
# 6) Write outputs
# ----------------------------
write_csv(overall_props, file.path(out_dir_data, "state_province_proportions.csv"))
write_csv(device_props,  file.path(out_dir_data, "device_state_province.csv"))
write_csv(dev_metrics,   file.path(out_dir_data, "device_migration_metrics.csv"))
writeLines(md, out_md)

# JSON summary for dashboards
json_out <- list(
  window_start_utc = format(start_utc, tz = "UTC", usetz = TRUE),
  window_end_utc   = format(now_utc, tz = "UTC", usetz = TRUE),
  n_devices = n_devices,
  n_fixes   = n_fixes,
  overall_state_province = overall_props,
  migration_status = status_counts,
  leaders = leaders
)

writeLines(
  jsonlite::toJSON(json_out, pretty = TRUE, auto_unbox = TRUE),
  file.path(out_dir_data, "weekly_summary.json")
)

message("✅ Weekly migration pipeline complete.")
message(" - ", out_md)
message(" - ", file.path(out_dir_data, "state_province_proportions.csv"))
message(" - ", file.path(out_dir_data, "weekly_summary.json"))
