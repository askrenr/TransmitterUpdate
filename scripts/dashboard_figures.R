# ----------------------------
# Figures ported from MigrationDashboard (quarto/index.qmd)
# ----------------------------
# Same Movebank study, so these read the dashboard's PUBLISHED data rather than
# re-running its forecast model and annual snapshots:
#   outlook.json                     7-day group outlook (Migration outlook)
#   history/<yr>_daily_lat_mean.json past years' mean latitude (Latitude by day)
#   history/current_daily_lat.json   this year's per-bird daily latitude (0.1 deg in season)
#   history/<yr>_states.json, current_states.json  weekly state counts (Where are they?)
#   device_migration_metrics.csv     mortality_status, to pick active birds
# Each figure is an interactive plotly widget saved next to the report and
# embedded with an <iframe>. Widgets share one dependency folder (lib/), so the
# twice-daily commits only change the small per-figure files.
# Every section fails soft: if the dashboard data can't be read, the report
# says so and everything else still publishes.

DASH_DATA_URL <- Sys.getenv("DASH_DATA_URL",
                            unset = "https://askrenr.github.io/MigrationDashboard/data")

dash_json <- function(path, ...) {
  tryCatch(jsonlite::fromJSON(paste0(DASH_DATA_URL, "/", path), ...),
           error = function(e) NULL)
}

save_fig <- function(widget, name, out_dir_data) {
  path <- file.path(out_dir_data, paste0(name, ".html"))
  htmlwidgets::saveWidget(widget, file = path, selfcontained = FALSE, libdir = "lib",
                          title = name)
  file.path(basename(out_dir_data), basename(path))
}

embed <- function(rel, height) {
  sprintf('<iframe src="%s" width="100%%" height="%d" style="border:0" loading="lazy"></iframe>',
          rel, height)
}

unavailable <- function(what) paste0("_", what, " unavailable on this update._")

current_yr_str <- format(Sys.Date(), "%Y")
# Light-to-dark blue ramp over every finished year (same as the dashboard).
palette_years <- as.character(2021:(as.integer(current_yr_str) - 1))
year_palette  <- setNames(
  grDevices::colorRampPalette(c("#aacae8", "#2a5f9e"))(length(palette_years)),
  palette_years
)

# ---------- Migration outlook ----------
outlook_section <- function(out_dir_data) {
  outlook <- dash_json("outlook.json", simplifyVector = FALSE)
  hdr <- "## Migration outlook (next 7 days)"
  if (is.null(outlook)) return(c(hdr, "", unavailable("Outlook")))
  if (identical(outlook$in_season, FALSE)) {
    return(c(hdr, "", "Forecasts resume Oct 1. The 7-day outlook runs during the fall and winter season, Oct 1 to Jan 31."))
  }
  if (!isTRUE(outlook$available)) return(c(hdr, "", unavailable("Outlook")))

  # Expected counts are rounded to 1 dp upstream, so 0 means "below 0.05".
  fmt_n <- function(x) ifelse(is.na(x), "—", ifelse(x < 0.1, "<0.1", formatC(x, format = "f", digits = 1)))
  fmt_pct <- function(p) ifelse(is.na(p), "", ifelse(p < 0.01, "<1%", ifelse(p > 0.99, ">99%",
                                paste0(round(100 * p), "%"))))
  html_lt <- function(x) gsub("<", "&lt;", x, fixed = TRUE)
  half_up <- function(x) floor(x + 0.5)
  RISK_BIRDS <- "tracked birds not yet in the Lower Mississippi Valley"
  fmt_total <- function(x, n) {
    if (x < 0.1) return(paste0("almost none of the ", n, " ", RISK_BIRDS))
    if (x < 1)   return(paste0("fewer than 1 of the ", n, " ", RISK_BIRDS))
    paste0("about ", half_up(x), " of the ", n, " ", RISK_BIRDS)
  }
  ol_num <- function(d, k) { v <- d[[k]]; if (is.null(v)) NA_real_ else as.numeric(v) }

  days <- outlook$days
  dd <- data.frame(
    date   = as.Date(vapply(days, function(d) d$date, "")),
    low    = vapply(days, function(d) isTRUE(d$low_confidence), TRUE),
    sup_a  = vapply(days, function(d) isTRUE(d$suppressed_a), TRUE),
    sup_b  = vapply(days, function(d) isTRUE(d$suppressed_b), TRUE),
    moves  = vapply(days, ol_num, 0, k = "expected_moves"),
    p_any  = vapply(days, ol_num, 0, k = "p_any_move"),
    arr    = vapply(days, ol_num, 0, k = "expected_arrivals"),
    cum    = vapply(days, ol_num, 0, k = "cum_expected_arrivals")
  )
  n_a <- as.integer(outlook$n_at_risk_a)
  if (nrow(dd) == 0 || all(dd$sup_a)) {
    return(c(hdr, "", "Too few tracked birds to forecast. The outlook needs at least 5 birds with a recent fix that have not yet reached the Lower Mississippi Valley."))
  }
  ok_a <- !dd$sup_a
  ok_b <- !dd$sup_b
  any_b <- any(ok_b)
  tot_moves <- sum(dd$moves[ok_a])
  tot_arr <- if (any_b) tail(dd$cum[ok_b], 1) else NA
  gap_note <- function(ok) if (!all(ok)) paste0(" (", sum(!ok), " day",
    if (sum(!ok) > 1) "s" else "", " with too few birds left out)") else ""

  ebm <- outlook$expected_birds_moving
  head_moves <- if (!is.null(ebm)) paste0(
    "Next 7 days: ", fmt_total(as.numeric(ebm), n_a),
    " expected to make a big move south (150 km or more).")
  else paste0("Next 7 days: about ", formatC(tot_moves, format = "f", digits = 1),
              " big moves south (150 km or more) expected among the ", n_a,
              " ", RISK_BIRDS, gap_note(ok_a), ".")
  head_arr <- if (any_b) paste0(
    if (tot_arr < 0.1) "Almost none" else if (tot_arr < 1) "Fewer than 1" else paste0("About ", half_up(tot_arr)),
    " expected to arrive in the Lower Mississippi Valley", gap_note(ok_b), ".")
  else "Arrivals in the Lower Mississippi Valley: too few tracked birds to forecast."

  xlab <- paste0(format(dd$date, "%a"), "<br>", as.integer(format(dd$date, "%m")), "/",
                 as.integer(format(dd$date, "%d")))
  hi <- c(moves = "#c05030", arr = "#3a5a28")
  tint <- c(moves = "#ecc3b8", arr = "#c3d1b6")
  bar_marker <- function(k) list(
    color   = ifelse(dd$low, tint[[k]], hi[[k]]),
    pattern = list(shape = ifelse(dd$low, "/", ""), fgcolor = hi[[k]], fgopacity = 0.8,
                   size = 6, solidity = 0.3),
    line    = list(color = hi[[k]], width = 1)
  )
  y_moves <- ifelse(ok_a, dd$moves, NA)
  y_arr   <- ifelse(ok_b, dd$arr, NA)
  floor_of <- function(y) ifelse(is.na(y), 0, pmax(y, max(c(y, 0.5), na.rm = TRUE) * 0.012))

  p1 <- plotly::plot_ly(
    x = xlab, y = floor_of(y_moves), type = "bar", marker = bar_marker("moves"),
    text = ifelse(ok_a, paste0(fmt_pct(dd$p_any), "<br>chance"), "too few<br>birds"),
    textposition = "outside", constraintext = "none",
    cliponaxis = FALSE, textfont = list(size = 11, color = "#5a4a2a"),
    customdata = html_lt(ifelse(ok_a, fmt_n(dd$moves), "—")),
    hovertext = html_lt(ifelse(ok_a, paste0(
      "Chance at least one tracked bird makes a big move that night: ", fmt_pct(dd$p_any)),
      "Too few birds to forecast")),
    hovertemplate = "%{x}: about <b>%{customdata}</b> big moves south expected<br>%{hovertext}<extra></extra>",
    showlegend = FALSE
  ) %>% plotly::layout(yaxis = list(title = list(text = "Big moves south", font = list(size = 11)),
                                    rangemode = "tozero", fixedrange = TRUE,
                                    range = c(0, max(c(y_moves, 0.5), na.rm = TRUE) * 1.3)))
  plots <- list(p1)
  if (any_b) {
    p2 <- plotly::plot_ly(
      x = xlab, y = floor_of(y_arr), type = "bar", marker = bar_marker("arr"),
      text = ifelse(ok_b, fmt_n(dd$arr), "too few<br>birds"), textposition = "outside",
      cliponaxis = FALSE, textfont = list(size = 11, color = "#5a4a2a"),
      customdata = html_lt(ifelse(ok_b, fmt_n(dd$arr), "—")),
      hovertemplate = "%{x}: about <b>%{customdata}</b> new arrivals expected in the Lower Mississippi Valley<extra></extra>",
      showlegend = FALSE
    ) %>% plotly::layout(yaxis = list(title = list(text = "New arrivals", font = list(size = 11)),
                                      rangemode = "tozero", fixedrange = TRUE,
                                      range = c(0, max(c(y_arr, 0.5), na.rm = TRUE) * 1.35)))
    plots <- c(plots, list(p2))
  }
  lo <- which(dd$low)
  shade <- if (length(lo)) list(list(
    type = "rect", xref = "x", yref = "paper", layer = "below",
    x0 = min(lo) - 1.5, x1 = max(lo) - 0.5, y0 = 0, y1 = 1,
    fillcolor = "#f3eee2", line = list(width = 0)))
  shade_lab <- if (length(lo)) list(list(
    x = (min(lo) + max(lo)) / 2 - 1, y = 1.0, xref = "x", yref = "paper",
    yanchor = "bottom", showarrow = FALSE, text = "lower confidence",
    font = list(size = 10, color = "#8a7a5a")))
  fig <- plotly::subplot(plots, nrows = length(plots), shareX = TRUE, titleY = TRUE,
                         heights = if (length(plots) == 2) c(0.62, 0.38) else 1,
                         margin = 0.06) %>%
    plotly::layout(
      shapes = shade, annotations = shade_lab, bargap = 0.3,
      xaxis  = list(type = "category", categoryorder = "array", categoryarray = xlab,
                    tickangle = 0, tickfont = list(size = 10), fixedrange = TRUE),
      paper_bgcolor = "#ffffff", plot_bgcolor = "#ffffff",
      margin = list(l = 10, r = 10, t = 22, b = 10)
    ) %>%
    plotly::config(displayModeBar = FALSE)
  # subplot() drops layout shapes; attach them to the built figure instead.
  fig <- plotly::plotly_build(fig)
  fig$x$layout$shapes <- shade
  height <- if (any_b) 330 else 230
  fig$height <- height

  updated <- tryCatch(format(lubridate::with_tz(
    as.POSIXct(outlook$generated_utc, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    "America/Chicago"), "Updated %b %d, %I:%M %p CT"), error = function(e) NULL)
  updated <- gsub("(^| )0", "\\1", updated)

  rel <- save_fig(fig, "fig_outlook", out_dir_data)
  c(
    hdr, "",
    paste0("**", head_moves, "** ", head_arr), "",
    embed(rel, height + 20), "",
    paste0(
      "**% = chance at least one tracked bird makes a big move that night.** ",
      "Red bars: expected number of big moves south (150 km or more) each night. ",
      if (any_b) "Green bars: expected number of birds arriving in the Lower Mississippi Valley. " else "",
      "Days 4–7 (hatched) are lower confidence: wind forecasts beyond 3 days are less reliable."
    ), "",
    paste0("_Group forecast for the tracked birds only, from weather forecasts and past ",
           "fall movements; each bird is scored from its last known area.",
           if (!is.null(updated)) paste0(" ", updated, ".") else "", "_")
  )
}

# ---------- Latitude by day ----------
latitude_section <- function(out_dir_data, active_ids) {
  hdr <- "## Latitude by day"
  lat_data_raw <- dash_json("history/current_daily_lat.json")
  has_lat_data <- is.data.frame(lat_data_raw) && nrow(lat_data_raw) > 0 &&
                  all(c("date", "device_id", "lat") %in% names(lat_data_raw))
  hist_lat_list <- list()
  for (yr in palette_years) {
    hl <- dash_json(paste0("history/", yr, "_daily_lat_mean.json"))
    if (is.data.frame(hl) && nrow(hl) > 0 && all(c("date", "mean_lat") %in% names(hl))) {
      hl$date_display <- as.Date(paste0("2000-", format(as.Date(hl$date), "%m-%d")))
      hist_lat_list[[yr]] <- hl
    }
  }
  if (!has_lat_data && length(hist_lat_list) == 0) return(c(hdr, "", unavailable("Latitude data")))

  p_lat <- plotly::plot_ly()
  for (yr in names(hist_lat_list)) {
    col <- year_palette[yr]
    if (is.na(col)) col <- "#888888"
    p_lat <- p_lat %>% plotly::add_lines(
      data = hist_lat_list[[yr]], x = ~date_display, y = ~mean_lat, name = yr,
      line = list(color = col, width = 3, dash = "dash"), opacity = 0.9,
      hovertemplate = paste0(yr, " %{x|%b %d}: <b>%{y:.1f}°N</b><extra></extra>")
    )
  }
  if (has_lat_data) {
    lat_data_raw <- lat_data_raw %>%
      dplyr::mutate(date = as.Date(date),
                    date_display = as.Date(paste0("2000-", format(date, "%m-%d"))))
    # Individual active birds are muted context; one legend entry toggles them all.
    lat_data <- lat_data_raw %>% dplyr::filter(device_id %in% active_ids)
    sorted_ids <- sort(unique(lat_data$device_id))
    for (bird in sorted_ids) {
      p_lat <- p_lat %>% plotly::add_lines(
        data = lat_data[lat_data$device_id == bird, ], x = ~date_display, y = ~lat,
        name = paste0(current_yr_str, " · individual birds"), legendgroup = "current_birds",
        showlegend = identical(bird, sorted_ids[1]),
        line = list(color = "#b0aba3", width = 1), opacity = 0.5,
        hovertemplate = paste0(bird, " %{x|%b %d}: <b>%{y:.1f}°N</b><extra></extra>")
      )
    }
    # Current-year mean over every bird, drawn last so it sits on top.
    cur_mean <- lat_data_raw %>%
      dplyr::group_by(date, date_display) %>%
      dplyr::summarise(mean_lat = mean(lat, na.rm = TRUE), .groups = "drop")
    p_lat <- p_lat %>% plotly::add_lines(
      data = cur_mean, x = ~date_display, y = ~mean_lat, name = paste0(current_yr_str, " mean"),
      line = list(color = "#ff7a00", width = 4),
      hovertemplate = paste0(current_yr_str, " mean %{x|%b %d}: <b>%{y:.1f}°N</b><extra></extra>")
    )
  }
  p_lat <- p_lat %>% plotly::layout(
    xaxis = list(title = "", tickformat = "%b", showgrid = FALSE),
    yaxis = list(title = "Latitude (°N)"),
    showlegend = TRUE, legend = list(font = list(size = 11)),
    paper_bgcolor = "#f5f0e8", plot_bgcolor = "#f5f0e8",
    margin = list(l = 10, r = 10, t = 10, b = 40)
  ) %>% plotly::config(displayModeBar = FALSE)

  rel <- save_fig(p_lat, "fig_latitude", out_dir_data)
  c(hdr, "", embed(rel, 460), "",
    paste0("_Dashed lines: mean latitude of all tracked birds in past years. Orange: ",
           current_yr_str, " mean. Grey: each active bird this year. Click a legend entry to hide or show it._"))
}

# ---------- Where are they? ----------
states_section <- function(out_dir_data) {
  hdr <- "## Where are they? (this week vs. past years)"
  current_iso_week <- lubridate::isoweek(Sys.Date())
  load_states_for_week <- function(path, iso_week) {
    entries <- dash_json(path, simplifyVector = FALSE)
    if (is.null(entries) || length(entries) == 0) return(NULL)
    tryCatch({
      week_nums <- vapply(entries, function(e) lubridate::isoweek(as.Date(e$week_start)), numeric(1))
      idx <- which(week_nums == iso_week)
      if (length(idx) == 0) idx <- which.min(abs(week_nums - iso_week))
      entries[[idx[1]]]$states
    }, error = function(e) NULL)
  }
  rows <- list()
  # 2021 is left out, as on the dashboard.
  for (yr in c(setdiff(palette_years, "2021"), current_yr_str)) {
    path <- if (yr == current_yr_str) "history/current_states.json" else paste0("history/", yr, "_states.json")
    sm <- load_states_for_week(path, current_iso_week)
    for (st in names(sm)) {
      rows[[length(rows) + 1]] <- data.frame(state = st, year = yr, n = as.integer(sm[[st]]))
    }
  }
  if (length(rows) == 0) return(c(hdr, "", unavailable("State history")))
  states_df <- do.call(rbind, rows)
  years <- unique(states_df$year)
  wide <- tidyr_free_pivot(states_df, years)

  top <- head(wide[order(-wide[[if (current_yr_str %in% years) current_yr_str else years[1]]]), ], 10)
  ca_abbrev <- c("Alberta" = "AB", "British Columbia" = "BC", "Manitoba" = "MB",
                 "New Brunswick" = "NB", "Newfoundland and Labrador" = "NL",
                 "Northwest Territories" = "NT", "Nova Scotia" = "NS", "Nunavut" = "NU",
                 "Ontario" = "ON", "Prince Edward Island" = "PE", "Québec" = "QC",
                 "Quebec" = "QC", "Saskatchewan" = "SK", "Yukon" = "YT")
  abbrev <- c(setNames(state.abb, state.name), ca_abbrev)
  labels <- ifelse(top$state %in% names(abbrev), abbrev[top$state], top$state)

  p <- plotly::plot_ly()
  for (yr in years) {
    col <- if (yr == current_yr_str) "#c05030" else year_palette[yr]
    if (is.na(col)) col <- "#4a7fc1"
    p <- p %>% plotly::add_bars(
      x = labels, y = top[[yr]], name = yr, marker = list(color = col),
      hovertemplate = paste0("%{x}: %{y} transmitters (", yr, ")<extra></extra>")
    )
  }
  p <- p %>% plotly::layout(
    barmode = "group",
    xaxis = list(title = "State / Province", categoryorder = "array", categoryarray = labels),
    yaxis = list(title = "Transmitters"),
    paper_bgcolor = "#f5f0e8", plot_bgcolor = "#f5f0e8",
    margin = list(l = 10, r = 10, t = 10, b = 60),
    showlegend = TRUE, legend = list(orientation = "h", y = 1.08)
  ) %>% plotly::config(displayModeBar = FALSE)

  rel <- save_fig(p, "fig_states", out_dir_data)
  c(hdr, "", embed(rel, 440), "",
    paste0("_Transmitters by state/province in ISO week ", current_iso_week,
           " of each year (top 10 by this year's count). ", current_yr_str,
           " in red. Click a year in the legend to hide or show it._"))
}

# Long (state, year, n) -> one row per state, one column per year, 0-filled,
# rows sorted by total.
tidyr_free_pivot <- function(states_df, years) {
  wide <- data.frame(state = unique(states_df$state), stringsAsFactors = FALSE)
  for (yr in years) {
    d <- states_df[states_df$year == yr, ]
    wide[[yr]] <- d$n[match(wide$state, d$state)]
    wide[[yr]][is.na(wide[[yr]])] <- 0L
  }
  tot <- rowSums(wide[, years, drop = FALSE])
  wide[tot > 0, ][order(-tot[tot > 0]), ]
}

# Birds the dashboard's accelerometer rules call alive (its "Transmitters
# active"). Falls back to every bird in this report's window.
dashboard_active_ids <- function(fallback_ids) {
  m <- tryCatch(readr::read_csv(paste0(DASH_DATA_URL, "/device_migration_metrics.csv"),
                                col_types = readr::cols(.default = "c")),
                error = function(e) NULL)
  if (is.null(m) || !"mortality_status" %in% names(m)) return(fallback_ids)
  m$device_id[m$mortality_status %in% "alive"]
}

dashboard_figure_sections <- function(out_dir_data, fallback_ids) {
  one <- function(f, what) tryCatch(f(), error = function(e) {
    message("Dashboard figure failed (", what, "): ", conditionMessage(e))
    c(paste0("## ", what), "", unavailable(what))
  })
  active_ids <- dashboard_active_ids(fallback_ids)
  c(
    one(function() outlook_section(out_dir_data), "Migration outlook"), "",
    one(function() latitude_section(out_dir_data, active_ids), "Latitude by day"), "",
    one(function() states_section(out_dir_data), "Where are they?"), ""
  )
}
