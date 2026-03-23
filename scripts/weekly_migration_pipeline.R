#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(move)
  library(dplyr)
  library(lubridate)
  library(sf)
  library(rnaturalearth)
  library(ggplot2)
  library(plotly)
  library(htmlwidgets)
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
start_utc <- now_utc - days(30)

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
# 3) Assign state/province using Natural Earth data
# ----------------------------

pts <- st_as_sf(gps, coords = c("lon", "lat"), crs = 4326, remove = FALSE)

# 3) Assign state/province using Natural Earth data (without rnaturalearthhires)

# Download medium‑scale (1:50) admin‑level‑1 polygons from Natural Earth.
# This layer is provided in rnaturalearthdata and does not need rnaturalearthhires.
admin1_all <- rnaturalearth::ne_download(
  scale       = 50,               # 1:50‑million scale avoids the hires package
  type        = "states",         # admin‑level‑1 states/provinces
  category    = "cultural",
  returnclass = "sf"
)

# Filter to US and Canadian states/provinces and map the fields used later
admin1 <- admin1_all %>%
  dplyr::filter(iso_a2 %in% c("US", "CA")) %>%
  dplyr::transmute(
    country        = iso_a2,      # two‑letter country code
    state_province = name,        # state/province name
    state_abbrev   = postal       # postal abbreviation (may be NA)
  )

admin1 <- sf::st_make_valid(admin1)

pts    <- sf::st_as_sf(gps, coords = c("lon", "lat"), crs = 4326, remove = FALSE)
pts    <- sf::st_make_valid(pts)
pts_admin <- sf::st_join(sf::st_transform(pts, 4326),
                         sf::st_transform(admin1, 4326),
                         left = TRUE, largest = TRUE)

gps_admin <- pts_admin %>%
  st_drop_geometry() %>%
  mutate(
    state_province = if_else(!is.na(state_province), state_province, "Unknown"),
    state_abbrev   = if_else(!is.na(state_abbrev), state_abbrev, NA_character_)
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
# 5b) State-to-state movement summary
# ----------------------------
device_moves <- gps_admin %>%
  group_by(device_id) %>%
  summarise(
    first_state = first(state_province),
    last_state  = last(state_province),
    .groups = "drop"
  ) %>%
  filter(!is.na(first_state), !is.na(last_state))

move_counts <- device_moves %>%
  filter(first_state != last_state) %>%
  count(first_state, last_state, name = "n_devices") %>%
  arrange(desc(n_devices)) %>%
  mutate(transition = paste(first_state, "→", last_state))

# Generate bar plot of state-to-state movements and save as PNG
fig_path <- NULL
fig_rel <- NULL
if (nrow(move_counts) > 0) {
  move_plot <- ggplot(move_counts, aes(x = reorder(transition, -n_devices), y = n_devices)) +
    geom_col(fill = "steelblue") +
    labs(
      title = "Transmitter movements between states (last 7 days)",
      x = "State transition",
      y = "Number of transmitters"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  fig_path <- file.path(out_dir_data, "state_transitions.png")
  ggsave(fig_path, move_plot, width = 8, height = 4, dpi = 150)
  # Compute relative path from markdown file to the image for linking
  fig_rel <- file.path(basename(out_dir_data), basename(fig_path))
}

# ----------------------------
# 5c) Current state distribution and monthly time-slider
# ----------------------------
#
# Produce a bar chart of current transmitter locations by state/province (latest fix)
# and an interactive time-slider showing daily counts by state for the last 30 days.

current_fig_rel <- NULL
slider_rel      <- NULL

try({
  # Bar chart of current state distribution
  if (nrow(overall_props) > 0) {
    current_plot <- ggplot(overall_props, aes(x = reorder(state_province, -n_devices), y = n_devices)) +
      geom_col(fill = "steelblue") +
      labs(
        title = "Current transmitter locations by state/province (latest fix)",
        x = "State/Province",
        y = "Number of transmitters"
      ) +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1))
    current_fig_path <- file.path(out_dir_data, "current_state_distribution.png")
    ggsave(current_fig_path, current_plot, width = 8, height = 4, dpi = 150)
    current_fig_rel  <- file.path(basename(out_dir_data), basename(current_fig_path))
  }

  # Compute state counts per day for last 30 days (including today)
  start_month_utc <- now_utc - lubridate::days(30)
  gps_month <- gps_admin %>%
    filter(timestamp >= start_month_utc) %>%
    mutate(date = as.Date(timestamp, tz = "UTC")) %>%
    group_by(device_id, date) %>%
    slice_max(order_by = timestamp, n = 1, with_ties = FALSE) %>%
    ungroup()

  state_counts_month <- gps_month %>%
    count(date, state_province, name = "n_devices")

  if (nrow(state_counts_month) > 0) {
    # Order states by total count to ensure consistent x-axis ordering
    state_order <- state_counts_month %>%
      group_by(state_province) %>%
      summarise(total = sum(n_devices, na.rm = TRUE)) %>%
      arrange(desc(total)) %>%
      pull(state_province)
    state_counts_month <- state_counts_month %>%
      mutate(
        state_province = factor(state_province, levels = state_order),
        date = as.Date(date)
      )

    # Build interactive slider plot using plotly
    plot_slider <- state_counts_month %>%
      plot_ly(
        x = ~state_province,
        y = ~n_devices,
        color = ~state_province,
        type = "bar",
        frame = ~date,
        hovertemplate = "State: %{x}<br>Devices: %{y}<extra></extra>"
      ) %>%
      layout(
        title = "Transmitter locations by state/province over the last 30 days",
        xaxis = list(title = "State/Province"),
        yaxis = list(title = "Number of transmitters"),
        showlegend = FALSE
      ) %>%
      animation_opts(frame = 1000, transition = 500, easing = "linear", redraw = FALSE) %>%
      animation_slider(currentvalue = list(prefix = "Date: "))

    slider_path <- file.path(out_dir_data, "state_counts_slider.html")
    htmlwidgets::saveWidget(plot_slider, file = slider_path, selfcontained = TRUE)
    slider_rel  <- file.path(basename(out_dir_data), basename(slider_path))
  }
}, silent = TRUE)

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

# Append a section with the state-to-state transitions figure, if available
if (!is.null(fig_rel)) {
  md <- c(
    md,
    "## State-to-state movements (last 7 days)",
    "",
    paste0("![](", fig_rel, ")"),
    ""
  )
}

# Append a section with the current state distribution bar chart, if available
if (!is.null(current_fig_rel)) {
  md <- c(
    md,
    "## Current transmitter location distribution",
    "",
    paste0("![](", current_fig_rel, ")"),
    ""
  )
}

# Append a section with the 30-day time-slider for state distribution, if available
if (!is.null(slider_rel)) {
  md <- c(
    md,
    "## Transmitter location distribution over time (last 30 days)",
    "",
    paste0("[Interactive bar chart](", slider_rel, ")"),
    ""
  )
}

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
