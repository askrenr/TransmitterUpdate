#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(move)
  library(dplyr)
  library(lubridate)
  library(sf)
  library(maps)
  library(jsonlite)
  library(readr)
  library(stringr)
  library(tibble)
})

# ----------------------------
# 0) Parameters
# ----------------------------
study_id <- as.numeric(Sys.getenv("MOVEBANK_STUDY_ID", unset = "2665435998"))

# time window: last 7 days ending now (UTC)
now_utc   <- with_tz(Sys.time(), "UTC")
start_utc <- now_utc - days(7)

out_dir_data <- Sys.getenv("OUT_DIR_DATA", unset = "docs/data")
out_md       <- Sys.getenv("OUT_MD", unset = "docs/weekly_status.md")

dir.create(out_dir_data, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_md), recursive = TRUE, showWarnings = FALSE)

THRESH_NET_KM_MIGRATING    <- as.numeric(Sys.getenv("THRESH_NET_KM_MIGRATING", unset = "150"))
THRESH_LAT_CHANGE_DEG      <- as.numeric(Sys.getenv("THRESH_LAT_CHANGE_DEG", unset = "1.0"))
THRESH_NET_KM_TRANSITIONAL <- as.numeric(Sys.getenv("THRESH_NET_KM_TRANSITIONAL", unset = "60"))

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

# ----------------------------
# 2) Normalize key columns
# ----------------------------
col_map <- list(
  lon = c("location_long", "longitude", "Longitude", "lon", "LONGITUDE"),
  lat = c("location_lat",  "latitude",  "Latitude",  "lat", "LATITUDE"),
  ts  = c("timestamp", "Timestamp", "time", "UTC_datetime"),
  tag = c("tag_local_identifier", "tag_id", "device_id"),
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

if (is.na(tag_col)) tag_col <- ind_col
if (is.na(tag_col)) stop("Could not find a transmitter identifier column.")

gps <- df %>%
  transmute(
    device_id = as.character(.data[[tag_col]]),
    individual_id = if (!is.na(ind_col)) as.character(.data[[ind_col]]) else NA_character_,
    timestamp = as.POSIXct(.data[[ts_col]], tz = "UTC"),
    lon = as.numeric(.data[[lon_col]]),
    lat = as.numeric(.data[[lat_col]])
  ) %>%
  filter(!is.na(device_id), !is.na(timestamp), !is.na(lon), !is.na(lat)) %>%
  arrange(device_id, timestamp)

if (nrow(gps) == 0) {
  stop("No GPS records returned for the last 7 days.")
}

# ----------------------------
# 3) Assign state/province using maps package
# ----------------------------

pts <- st_as_sf(gps, coords = c("lon", "lat"), crs = 4326, remove = FALSE)

world_map <- maps::map("world", fill = TRUE, plot = FALSE)
admin1_raw <- sf::st_as_sf(world_map)

# Force CRS to WGS84
if (is.na(st_crs(admin1_raw))) {
  st_crs(admin1_raw) <- 4326
} else {
  admin1_raw <- st_transform(admin1_raw, 4326)
}

if (is.na(st_crs(pts))) {
  st_crs(pts) <- 4326
} else {
  pts <- st_transform(pts, 4326)
}

admin1 <- admin1_raw %>%
  mutate(ID = as.character(ID)) %>%
  filter(str_detect(ID, "^(USA|Canada):")) %>%
  mutate(
    country = str_extract(ID, "^[^:]+"),
    name = str_replace(ID, "^[^:]+:", ""),
    name = str_replace_all(name, "_", " ")
  ) %>%
  dplyr::select(country, name)

admin_lookup <- tribble(
  ~country,  ~name,                       ~state_abbrev,
  "USA",     "Alabama",                   "AL",
  "USA",     "Alaska",                    "AK",
  "USA",     "Arizona",                   "AZ",
  "USA",     "Arkansas",                  "AR",
  "USA",     "California",                "CA",
  "USA",     "Colorado",                  "CO",
  "USA",     "Connecticut",               "CT",
  "USA",     "Delaware",                  "DE",
  "USA",     "District of Columbia",      "DC",
  "USA",     "Florida",                   "FL",
  "USA",     "Georgia",                   "GA",
  "USA",     "Hawaii",                    "HI",
  "USA",     "Idaho",                     "ID",
  "USA",     "Illinois",                  "IL",
  "USA",     "Indiana",                   "IN",
  "USA",     "Iowa",                      "IA",
  "USA",     "Kansas",                    "KS",
  "USA",     "Kentucky",                  "KY",
  "USA",     "Louisiana",                 "LA",
  "USA",     "Maine",                     "ME",
  "USA",     "Maryland",                  "MD",
  "USA",     "Massachusetts",             "MA",
  "USA",     "Michigan",                  "MI",
  "USA",     "Minnesota",                 "MN",
  "USA",     "Mississippi",               "MS",
  "USA",     "Missouri",                  "MO",
  "USA",     "Montana",                   "MT",
  "USA",     "Nebraska",                  "NE",
  "USA",     "Nevada",                    "NV",
  "USA",     "New Hampshire",             "NH",
  "USA",     "New Jersey",                "NJ",
  "USA",     "New Mexico",                "NM",
  "USA",     "New York",                  "NY",
  "USA",     "North Carolina",            "NC",
  "USA",     "North Dakota",              "ND",
  "USA",     "Ohio",                      "OH",
  "USA",     "Oklahoma",                  "OK",
  "USA",     "Oregon",                    "OR",
  "USA",     "Pennsylvania",              "PA",
  "USA",     "Rhode Island",              "RI",
  "USA",     "South Carolina",            "SC",
  "USA",     "South Dakota",              "SD",
  "USA",     "Tennessee",                 "TN",
  "USA",     "Texas",                     "TX",
  "USA",     "Utah",                      "UT",
  "USA",     "Vermont",                   "VT",
  "USA",     "Virginia",                  "VA",
  "USA",     "Washington",                "WA",
  "USA",     "West Virginia",             "WV",
  "USA",     "Wisconsin",                 "WI",
  "USA",     "Wyoming",                   "WY",
  "Canada",  "Alberta",                   "AB",
  "Canada",  "British Columbia",          "BC",
  "Canada",  "Manitoba",                  "MB",
  "Canada",  "New Brunswick",             "NB",
  "Canada",  "Newfoundland",              "NL",
  "Canada",  "Labrador",                  "NL",
  "Canada",  "Newfoundland and Labrador", "NL",
  "Canada",  "Nova Scotia",               "NS",
  "Canada",  "Ontario",                   "ON",
  "Canada",  "Prince Edward Island",      "PE",
  "Canada",  "Quebec",                    "QC",
  "Canada",  "Saskatchewan",              "SK",
  "Canada",  "Northwest Territories",     "NT",
  "Canada",  "Nunavut",                   "NU",
  "Canada",  "Yukon",                     "YT",
  "Canada",  "Yukon Territory",           "YT"
)

admin1 <- admin1 %>%
  left_join(admin_lookup, by = c("country", "name"))

admin1 <- st_make_valid(admin1)
pts    <- st_make_valid(pts)

if (is.na(st_crs(admin1))) st_crs(admin1) <- 4326
if (is.na(st_crs(pts))) st_crs(pts) <- 4326

admin1 <- st_transform(admin1, 4326)
pts    <- st_transform(pts, 4326)

pts_admin <- st_join(pts, admin1, left = TRUE, largest = TRUE)

gps_admin <- pts_admin %>%
  st_drop_geometry() %>%
  mutate(
    state_province = if_else(!is.na(name), name, "Unknown"),
    state_abbrev   = state_abbrev
  )

# ----------------------------
# 4) Current location by device (latest fix only)
# ----------------------------
device_current <- gps_admin %>%
  group_by(device_id) %>%
  slice_max(order_by = timestamp, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  mutate(
    window_start_utc = start_utc,
    window_end_utc   = now_utc
  )

overall_props <- device_current %>%
  count(state_province, sort = TRUE, name = "n_devices") %>%
  mutate(
    prop = n_devices / sum(n_devices),
    window_start_utc = start_utc,
    window_end_utc   = now_utc
  )

device_props <- device_current %>%
  dplyr::select(
    device_id,
    individual_id,
    timestamp,
    lon,
    lat,
    state_province,
    state_abbrev,
    window_start_utc,
    window_end_utc
  ) %>%
  arrange(state_province, device_id)

# ----------------------------
# 5) Migration metrics per device
# ----------------------------
haversine_km <- function(lon1, lat1, lon2, lat2) {
  to_rad <- function(x) x * pi / 180
  R <- 6371.0088
  dlat <- to_rad(lat2 - lat1)
  dlon <- to_rad(lon2 - lon1)
  a <- sin(dlat / 2)^2 + cos(to_rad(lat1)) * cos(to_rad(lat2)) * sin(dlon / 2)^2
  2 * R * asin(pmin(1, sqrt(a)))
}

dev_metrics <- gps_admin %>%
  group_by(device_id) %>%
  arrange(timestamp, .by_group = TRUE) %>%
  summarise(
    n_fixes = n(),
    start_time = first(timestamp),
    end_time   = last(timestamp),
    start_lon  = first(lon),
    start_lat  = first(lat),
    end_lon    = last(lon),
    end_lat    = last(lat),
    net_km     = haversine_km(start_lon, start_lat, end_lon, end_lat),
    lat_change = end_lat - start_lat,
    max_lat    = max(lat, na.rm = TRUE),
    min_lat    = min(lat, na.rm = TRUE),
    top_state = {
      tab <- sort(table(state_province), decreasing = TRUE)
      if (length(tab) == 0) NA_character_ else names(tab)[1]
    },
    top_state_prop = {
      tab <- sort(table(state_province), decreasing = TRUE)
      if (length(tab) == 0) NA_real_ else as.numeric(tab[1] / sum(tab))
    },
    current_state = last(state_province),
    current_state_abbrev = last(state_abbrev),
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
# 6) Narrative generator
# ----------------------------
fmt_pct <- function(x) paste0(round(100 * x, 1), "%")
fmt_km  <- function(x) format(round(x), big.mark = ",")

n_devices <- n_distinct(device_current$device_id)
n_fixes   <- nrow(gps_admin)

status_counts <- dev_metrics %>%
  count(migration_status) %>%
  mutate(prop = n / sum(n)) %>%
  arrange(match(migration_status, c("Local", "Transitional", "Migrating")))

top_states <- overall_props %>% slice_head(n = 6)
leaders    <- dev_metrics %>% slice_head(n = min(3, nrow(dev_metrics)))

window_str <- paste0(
  format(start_utc, "%Y-%m-%d"), " to ", format(now_utc, "%Y-%m-%d"), " (UTC)"
)

state_section <- if (nrow(top_states) > 0) {
  paste0(
    "- ",
    top_states$state_province,
    ": ",
    fmt_pct(top_states$prop),
    " (n=",
    top_states$n_devices,
    ")"
  )
} else {
  "- No state/province assignments available."
}

status_section <- if (nrow(status_counts) > 0) {
  paste0(
    "- ",
    status_counts$migration_status,
    ": ",
    status_counts$n,
    " (",
    fmt_pct(status_counts$prop),
    ")"
  )
} else {
  "- No migration status available."
}

leaders_section <- if (nrow(leaders) > 0) {
  paste0(
    "- ",
    leaders$device_id,
    ": **",
    fmt_km(leaders$net_km),
    " km** net, lat change **",
    round(leaders$lat_change, 2),
    "°**, current admin area **",
    leaders$current_state,
    "**"
  )
} else {
  "- No leader summaries available."
}

md <- c(
  "# Weekly Mallard Migration Status",
  "",
  paste0("**Window:** ", window_str),
  paste0("**Transmitters reporting:** ", n_devices, "  |  **GPS fixes:** ", format(n_fixes, big.mark = ",")),
  "",
  "## Current state/province distribution (latest fix per transmitter)",
  "",
  state_section,
  "",
  "## Migration status (by transmitter)",
  "",
  status_section,
  "",
  "## Notes",
  "",
  paste0(
    "State/province percentages reflect the **current location of each transmitter** based on its most recent GPS fix within the last 7 days, rather than the total number of GPS fixes collected. ",
    "Most transmitters continue to show **local late-winter movements** within primary wintering areas. ",
    "A subset of birds are showing **transitional behavior** (larger net displacement without strong latitudinal gain), ",
    "and the **leading edge of spring migration** is evident among birds with substantial northward movement over the last week."
  ),
  "",
  "## Leading movement this week (net displacement)",
  "",
  leaders_section,
  ""
)

# ----------------------------
# 7) Write outputs
# ----------------------------
write_csv(overall_props, file.path(out_dir_data, "state_province_proportions.csv"))
write_csv(device_props,  file.path(out_dir_data, "device_state_province.csv"))
write_csv(dev_metrics,   file.path(out_dir_data, "device_migration_metrics.csv"))
writeLines(md, out_md)

json_out <- list(
  window_start_utc = format(start_utc, tz = "UTC", usetz = TRUE),
  window_end_utc   = format(now_utc, tz = "UTC", usetz = TRUE),
  n_devices = n_devices,
  n_fixes   = n_fixes,
  overall_state_province = overall_props,
  current_device_locations = device_props,
  migration_status = status_counts,
  leaders = leaders
)

writeLines(
  jsonlite::toJSON(json_out, pretty = TRUE, auto_unbox = TRUE, null = "null"),
  file.path(out_dir_data, "weekly_summary.json")
)

message("✅ Weekly migration pipeline complete.")
message(" - ", out_md)
message(" - ", file.path(out_dir_data, "state_province_proportions.csv"))
message(" - ", file.path(out_dir_data, "weekly_summary.json"))