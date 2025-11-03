# rm(list = ls())
library(data.table)
library(ggplot2)
library(finutils)
library(roll)
library(PerformanceAnalytics)
library(AzureStor)
library(arrow)
library(patchwork)
library(qlcal)
library(arrow)
library(lubridate)

# TODO:
# 1) sum 1/0 (buy/sell) and buy stock with highest number of buys
# 2) weighted mean of zscore.
# 3) not mean of zscore but sum

# Setup
PATH_SAVE = "H:/strategies/statsarb"
PATH_LEAN = "C:/Users/Mislav/qc_snp/data"
calendars
setCalendar("UnitedStates/NYSE")


# DATA --------------------------------------------------------------------
# Import daily data
prices = qc_daily_parquet(
  file_path = file.path(PATH_LEAN, "all_stocks_daily"),
  min_obs = 2 * 252,
  price_threshold = 1e-008,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  etfs = FALSE
)
# prices[, q := data.table::yearqtr(date)]

# Symbol to uppercase
prices[, symbol := toupper(symbol)]

# Add target columns
prices[, ret_1 := shift(close, 1L, type = "lead") / close - 1, by = symbol]
prices[, ret_5 := shift(close, 5L, type = "lead") / close - 1, by = symbol]
prices[, ret_10 := shift(close, 10L, type = "lead") / close - 1, by = symbol]
prices[, ret_22 := shift(close, 22L, type = "lead") / close - 1, by = symbol]
prices[, ret_66 := shift(close, 66L, type = "lead") / close - 1, by = symbol]
prices[, ret_1_o_o := shift(open, 2L, type = "lead") / shift(open, 1L, type = "lead") - 1, by = symbol]
prices[, ret_1_o_c := shift(close, 1L, type = "lead") / shift(open, 1L, type = "lead") - 1, by = symbol]
prices[, ret_2_o_c := shift(close, 2L, type = "lead") / shift(open, 1L, type = "lead") - 1, by = symbol]

# Import pairs
pairs = read_feather(file.path(PATH_SAVE, "pairs_combo.feather"))


# FILTERING ---------------------------------------------------------------
# Filter pairs
pairs = pairs[
  same_sector == 1 &
    isFund1 == FALSE & isFund2 == FALSE &
    isEtf1 == FALSE & isEtf2 == FALSE &
    companyName1 != companyName2]

# Remove symbols not in prices (BRK)
pairs_symbols = unique(c(pairs[, stock1], pairs[, stock2]))
remove_symbols = pairs_symbols[!(pairs_symbols %in% prices[, unique(symbol)])]
pairs = pairs[stock1 %notin% remove_symbols]
pairs = pairs[stock2 %notin% remove_symbols]

# Descriptive
pairs[, .N, by = date][order(date)] |>
  ggplot(aes(x = date, y = N)) +
  geom_bar(stat = "identity") +
  scale_x_continuous(breaks = seq(0, 1, by = 0.1)) +
  theme_minimal() +
  labs(title = "Number of pairs by quarter", x = "Quantile", y = "Number of pairs")

# Parameters
np  = 0.02

# Select n best by var
setorder(pairs, date, -combo)
dt = pairs[, head(.SD, as.integer(np * length(stock1))), by = date]

# At least n rows
n_ = 10
keep_symbols = dt[, .(symbol = c(stock1, stock2))] |>
  _[, .N, by = symbol] |>
  _[N >= n_] |>
  _[, symbol]
removed = setdiff(dt[, unique(c(stock1, stock2))], keep_symbols)
print(paste("Removed", length(removed), "symbols with less than 50 occurrences",
            "and", length(keep_symbols), "symbols kept"))

# Prices to long format to calculate z score more easly
prices_long = prices[, .(symbol = toupper(symbol), date, close)]
prices_long = prices_long[symbol %in% pairs[, stock1] | symbol %in% pairs[, stock2]]

# Calculate Zscore for every quarter-day
dt = dt[, .(date, stock1, stock2)]
dates = dt[, sort(unique(date))]
universe = list()
for (i in seq_along(dates)) {
  # i = 5
  # DEBUG
  print(i)
  if (i == length(dates)) break

  date_ = dates[i]
  date_start_ = date_ - 200
  if (i == length(dates)) {
    p = prices[date %between% c(date_start_, Sys.Date())]
  } else {
    p = prices[date %between% c(date_start_, dates[i+1])]
  }
  # Test
  # all(dt[date == date_, stock1] %in% colnames(p))
  # anyDuplicated(dt[date == date_, stock1])

  # 1) Unique pairs for this anchor date
  pairs_ = dt[date == date_, .(stock1, stock2)]

  # 2) All dates in the current window
  dates_win = sort(unique(p$date))

  # 3) Build grid = all (pair, date) combinations  (safe, explicit cartesian)
  pairs_[, dummy := 1L]
  grid = pairs_[
    data.table(date = dates_win, dummy = 1L),
    on = "dummy", allow.cartesian = TRUE
  ][, dummy := NULL][]
  # any(is.na(grid))

  # 4) Prepare closes for stock1 and stock2 (filter by needed symbols only)
  syms1 = unique(pairs_$stock1)
  syms2 = unique(pairs_$stock2)

  dt_1 = p[symbol %in% syms1, .(date, stock1 = symbol, close_1 = close)]
  dt_2 = p[symbol %in% syms2, .(date, stock2 = symbol, close_2 = close)]

  setkey(dt_1, date, stock1)
  setkey(dt_2, date, stock2)

  # 5) Join closes onto the grid
  res = grid[
    dt_1, on = .(date, stock1)
  ][
    dt_2, on = .(date, stock2)
  ]
  setorder(res, stock1, stock2)
  # any(is.na(res))
  # Calculate Z scores
  res[, ratiospread := close_1 / close_2]
  res[, spreadclose := log(ratiospread)]
  res[, let(
    sma20  = frollmean(spreadclose, 20, na.rm = TRUE),
    sma60  = frollmean(spreadclose, 60, na.rm = TRUE),
    sma120 = frollmean(spreadclose, 120, na.rm = TRUE),
    sd20   = roll_sd(spreadclose, 20),
    sd60   = roll_sd(spreadclose, 60),
    sd120  = roll_sd(spreadclose, 120)
  ), by = .(stock1, stock2)]
  res[, let(
    zscore_20  = (spreadclose - sma20) / sd20,
    zscore_60  = (spreadclose - sma60) / sd60,
    zscore_120 = (spreadclose - sma120) / sd120
  )]
  res[, zscore := (zscore_20 + zscore_60 + zscore_120) / 3]

  # Calculate percent deviation
  res[, rel_ret_10 := (close_1 / shift(close_1, 10)) / (close_2 / shift(close_2, 10)) - 1]

  if (i == length(dates)) {
    res = res[date %between% c(date_, Sys.Date()), .(date, stock1, stock2, zscore, rel_ret_10)]
  } else {
    res = res[date %between% c(date_, dates[i+1]), .(date, stock1, stock2, zscore, rel_ret_10)]
  }
  universe[[i]] = res
}
universe = rbindlist(universe)

# Remove missing values
unvierse = na.omit(universe)

# Prepare data for QC
mean_zscore = rbind(
  universe[, .(date, stock = stock1, zscore)],
  universe[, .(date, stock = stock2, zscore = -zscore)]
) |>
  _[, .(
    mean_zscore    = mean(zscore, na.rm = TRUE),
    sum_zscore_bin = sum(zscore > 0, na.rm = TRUE) - sum(zscore <= 0, na.rm = TRUE),
    wmean_zscore   = weighted.mean(zscore, w = rev(1:length(zscore) / sum(1:length(zscore))), na.rm = TRUE)
  ),by = .(date, stock)]
mean_rel_ret_10 = rbind(
  universe[, .(date, stock = stock1, rel_ret_10)],
  universe[, .(date, stock = stock2, rel_ret_10 = -rel_ret_10)]
)[, .(mean_rel_ret_10 = mean(rel_ret_10, na.rm = TRUE)), by = .(date, stock)]
mean_indicators = mean_zscore[mean_rel_ret_10, on = c("stock", "date")]
mean_indicators = na.omit(mean_indicators)

# Merge prices again
portfolio = prices[, .(symbol, date, returns, ret_1, ret_5, ret_10, ret_22)] |>
  _[mean_indicators, on = c(symbol = "stock", "date")] |>
  na.omit()

# Correlation
portfolio[, cor(mean_zscore, mean_rel_ret_10)]
portfolio[, cor(mean_zscore, wmean_zscore)]
portfolio[, cor(mean_zscore, sum_zscore_bin)]

# Plot regression
plots_reg = function(ret_target = "ret_1") {
  y <- sym(ret_target)
  g1 = ggplot(portfolio, aes(mean_zscore, !!y)) +
    geom_smooth(method = "lm")
  g2 = ggplot(portfolio, aes(mean_zscore, !!y)) +
    geom_smooth(method = "gam")
  g3 = ggplot(portfolio, aes(mean_rel_ret_10, !!y)) +
    geom_smooth(method = "lm")
  g4 = ggplot(portfolio, aes(mean_rel_ret_10, !!y)) +
    geom_smooth(method = "gam")
  (g1 + g3) / (g2 + g4)
}
plots_reg()
plots_reg("ret_5")
# plots_reg("ret_10")
# plots_reg("ret_22")

# Plot deciles
portfolio[, .(bin = dplyr::ntile(mean_zscore, 20), ret_1), by = date] |>
  _[, .(ret = mean(ret_1)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")
portfolio[, .(bin = dplyr::ntile(mean_rel_ret_10, 20), ret_1), by = date] |>
  _[, .(ret = mean(ret_1)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")
portfolio[, .(bin = dplyr::ntile(sum_zscore_bin, 20), ret_1), by = date] |>
  _[, .(ret = mean(ret_1)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")

portfolio[, .(bin = dplyr::ntile(mean_zscore, 20), ret_5), by = date] |>
  _[, .(ret = mean(ret_5)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")
portfolio[, .(bin = dplyr::ntile(mean_rel_ret_10, 20), ret_5), by = date] |>
  _[, .(ret = mean(ret_5)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")
portfolio[, .(bin = dplyr::ntile(mean_zscore, 20), ret_10), by = date] |>
  _[, .(ret = mean(ret_10)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")
portfolio[, .(bin = dplyr::ntile(mean_rel_ret_10, 20), ret_10), by = date] |>
  _[, .(ret = mean(ret_10)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")
portfolio[, .(bin = dplyr::ntile(mean_zscore, 20), ret_22), by = date] |>
  _[, .(ret = mean(ret_22)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")
portfolio[, .(bin = dplyr::ntile(mean_rel_ret_10, 20), ret_22), by = date] |>
  _[, .(ret = mean(ret_22)), by = .(bin)] |>
  ggplot(aes(bin, ret)) +
  geom_bar(stat = "identity")


# BACKTEST DAILY ---------------------------------------------------------
# Backtest - CS with DAILY rebalancing
back = prices[, .(symbol, date, returns, ret_1, ret_1_o_o, ret_1_o_c)] |>
  _[mean_indicators, on = c(symbol = "stock", "date")] |>
  na.omit() |>
  _[data.table(symbol = keep_symbols)[, keep := 1], on = "symbol"] |>
  na.omit()
REBALANCE = "day" # day, week or month
# back = back[close_raw > 5]
if (REBALANCE == "week") {
  back[, date := lubridate::ceiling_date(date, unit = "week")]
} else if (REBALANCE == "month") {
  back[, date := data.table::yearmon(date)]
}
if (REBALANCE %in% c("week", "month")) {
  back = back[, .(
    open            = data.table::first(open),
    close           = data.table::last(close),
    mean_zscore     = data.table::last(mean_zscore),
    mean_rel_ret_10 = data.table::last(mean_rel_ret_10),
    sum_zscore_bin  = data.table::last(sum_zscore_bin)
  ), by = .(symbol, date)]
}
back[, combo := ((frank(mean_zscore, ties.method = "first") / length(mean_zscore)) +
                   (frank(mean_rel_ret_10, ties.method = "first")  / length(mean_zscore))) / 2,
     by = date]

back[, table(keep)]
setorder(back, date, -wmean_zscore)
back_short = back[, head(.SD, 50), by = date]
back_long  = back[, tail(.SD, 50), by = date]
# setorder(back_long, date, -mean_rel_ret_10)
# back_long = back_long[, tail(.SD, 50), by = date]
# Long
long_xts = back_long[, .(strategy = sum((1/length(ret_1)) * ret_1_o_o)), by = date] |>
  as.xts.data.table()
charts.PerformanceSummary(long_xts)
SharpeRatio.annualized(long_xts, scale = 252)
# SharpeRatio.annualized(long_xts["2021/"], scale = 252)
Return.cumulative(long_xts)
Return.annualized(long_xts, scale = 252)
StdDev.annualized(long_xts, scale = 252)
# Short
short_xts = back_short[, .(strategy = sum(-(1/length(ret_1)) * ret_1_o_o)), by = date] |>
  as.xts.data.table()
charts.PerformanceSummary(short_xts)
SharpeRatio.annualized(short_xts, scale = 252)
# Long short
long_short = cbind(long_xts, short_xts)
long_short$strategy_combo = (long_short$strategy/2) + (long_short$`strategy.1` / 2)
charts.PerformanceSummary(long_short)
Return.annualized(long_short, scale = 252)
SharpeRatio.annualized(long_short, scale = 252)
lapply(Drawdowns(long_short), min)
# Save symbols for Quantconnect
qc_data = back_long[, .(date, symbol, weight = 1/50)]
qc_data[, date_adv := vapply(date, qlcal::advanceDate, days = 1L, numeric(1L))]
qc_data[, date_adv := as.Date(date_adv)]
qc_data[, date := as.character(date_adv)]
qc_data[, date_adv := NULL]
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
cont = storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(as.data.frame(qc_data), cont, "pairs_ensamble.csv")
# Comapre local and qc
head(qc_data[as.Date(date) > as.Date("2023-01-01")], 55) |>
  _[order(date, symbol)]


# BACKTEST WEEKLY ---------------------------------------------------------
# Backtest - CS with weekly rebalancing
back = prices[, .(symbol, date, returns, ret_1, close, open, close_raw)] |>
  _[mean_indicators, on = c(symbol = "stock", "date")] |>
  na.omit()
REBALANCE = "week" # week or month
back = back[close_raw > 5]
if (REBALANCE == "week") {
  back[, time_event := lubridate::ceiling_date(date, unit = "week")]
} else if (REBALANCE == "month") {
  back[, time_event := data.table::yearmon(date)]
}
back[, unique(time_event)]
back = back[, .(
  date            = data.table::last(date),
  open            = data.table::first(open),
  close           = data.table::last(close),
  mean_zscore     = data.table::last(mean_zscore),
  mean_rel_ret_10 = data.table::last(mean_rel_ret_10),
  sum_zscore_bin  = data.table::last(sum_zscore_bin)
), by = .(symbol, time_event)]
back[, combo := ((frank(mean_zscore, ties.method = "first") / length(mean_zscore)) +
                   (frank(mean_rel_ret_10, ties.method = "first")  / length(mean_zscore))) / 2,
     by = time_event]
back[, ret_1 := close / open - 1]
setorder(back, symbol, time_event)
back[, ret_1 := shift(ret_1, 1L, type = "lead"), by = symbol]
back = na.omit(back)
setorder(back, time_event, -mean_zscore)
back_short = back[, head(.SD, 50), by = time_event]
back_long  = back[, tail(.SD, 50), by = time_event]
# back_long  = back[, .SD[mean_zscore < -3], by = time_event]
# Long
long_xts = back_long[, .(strategy = sum((1/length(ret_1)) * ret_1) - 0.001), by = time_event]
if (REBALANCE == "month") {
  long_xts[, time_event := zoo::as.yearmon.default(time_event)]
}
long_xts = as.xts.data.table(long_xts)
charts.PerformanceSummary(long_xts)
SharpeRatio.annualized(long_xts, scale = if (REBALANCE == "week") 52 else 12)
Return.annualized(long_xts, scale = if (REBALANCE == "week") 52 else 12)
StdDev.annualized(long_xts, scale = if (REBALANCE == "week") 52 else 12)
# Shortshort_xts = back_short[, .(strategy = sum(-(1/length(ret_1)) * ret_1)), by = time_event]
short_xts = back_short[, .(strategy = sum((1/length(ret_1)) * ret_1)), by = time_event]
if (REBALANCE == "month") short_xts[, time_event := zoo::as.yearmon.default(time_event)]
short_xts = as.xts.data.table(short_xts)
charts.PerformanceSummary(short_xts)
SharpeRatio.annualized(short_xts, scale = if (REBALANCE == "week") 52 else 12)
Return.annualized(short_xts, scale = if (REBALANCE == "week") 52 else 12)
# Long short
long_short = cbind(long_xts, short_xts)
long_short$strategy_combo = (long_short$strategy) + (long_short$`strategy.1`)
charts.PerformanceSummary(long_short)
Return.annualized(long_short, scale = if (REBALANCE == "week") 52 else 12)
SharpeRatio.annualized(long_short, scale = if (REBALANCE == "week") 52 else 12)
lapply(Drawdowns(long_short), min)
# Save symbols for Quantconnect
# qc_data = rbind(
#   back_long[, .(date = time_event, symbol, weight = 1/20)],
#   back_short[, .(date = time_event, symbol, weight = -1/20)]
# )
if (REBALANCE == "month") {
  qc_data = back_long[, .(month = time_event, date, symbol, weight = 1/10)]
  qc_data[, date_month := zoo::as.Date.yearmon(month, frac = 1)]
} else if (REBALANCE == "week") {
  qc_data = back_long[, .(date, symbol, weight = 1/30)]
}
qc_data[, sort(unique(date))]
qc_data[, date := as.character(date_month)]
qc_data = qc_data[, .(date, symbol, weight)]
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
cont = storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(as.data.frame(qc_data), cont, "pairs_ensamble.csv")
# Compare QC and local
setorder(qc_data, date, symbol)
qc_data[, unique(date)]

qc_data[date == "2020-08-02"]
qc_data[date == "2020-08-09"]


# TIME SERIES REBALANCING -------------------------------------------------
# Backtest - TS with weekly rebalancing
back = prices[, .(symbol, date, returns, ret_1)] |>
  _[mean_indicators, on = c(symbol = "stock", "date")] |>
  na.omit()
back[, combo := ((frank(mean_zscore, ties.method = "first") / length(mean_zscore)) +
                   (frank(mean_rel_ret_10, ties.method = "first")  / length(mean_zscore))) / 2,
     by = date]
setorder(back, symbol, date)
# --- PARAMETERS ---
entry_long  = -1
exit_long   =  0
entry_short =  2.0
exit_short  =  1
do_short        = FALSE    # <- toggle short side
tc_bps_per_turn = 5        # transaction costs (per entry/exit) in bps; set 0 to disable
dollar_neutral  = FALSE     # cross-sectional demeaning of positions by day
# Long
back[, pos_long := fifelse(mean_zscore <= entry_long, 1,
                           fifelse(mean_zscore >= exit_long, 0, NA_real_)),
     by = symbol]
back[, .N, by = pos_long]
back[, pos_long := na.locf(pos_long, na.rm = FALSE), by = symbol]
back[, .N, by = pos_long]
back[is.na(pos_long), pos_long := 0]
back[pos_long == 1]
# Short
back[, pos_short := fifelse(mean_zscore >= entry_short, -1,
                            fifelse(mean_zscore <= exit_short, 0, NA_real_)),
     by = symbol]
back[, pos_short := na.locf(pos_short, na.rm = FALSE), by = symbol]
back[is.na(pos_short), pos_short := 0]
# Combine
back[, position := if (do_short) pos_long + pos_short else pos_long]
# Optional: daily dollar neutrality (demean positions each day)
if (dollar_neutral) {
  back[, position := position - mean(position), by = date]
}
# --- STRATEGY RETURNS (lagged position * next-day return) ---
back[, strat_ret := shift(position, 1) * returns, by = symbol]
# --- TRANSACTION COSTS (per symbol when position changes) ---
if (tc_bps_per_turn > 0) {
  back[, turnover := abs(position - shift(position, 1, fill = 0)), by = symbol]
  back[, strat_ret := strat_ret - (tc_bps_per_turn / 10000) * turnover]
}
daily <- back[, .(strategy = mean(strat_ret, na.rm = TRUE)), by = date]
daily <- daily[!is.na(strategy)]
daily_xts <- as.xts.data.table(daily)
charts.PerformanceSummary(daily_xts)
SharpeRatio.annualized(daily_xts, scale = 252)
Return.annualized(daily_xts, scale = 252)
maxDrawdown(daily_xts)
StdDev.annualized(daily_xts, scale = 252)

# Test one symbol
back = prices[, .(symbol, date, returns, ret_1)] |>
  _[mean_indicators, on = c(symbol = "stock", "date")] |>
  na.omit()
setorder(back, symbol, date)
x_ = back[symbol == "T"]
hist(x_[, mean_zscore])
plot(x_[, mean_zscore])
x_[mean_zscore < -1]
x_[, pos_long := fifelse(mean_zscore <= -1, 1, fifelse(mean_zscore >= 0, 0, NA_real_))]
setnafill(x_, type = "locf", cols = "pos_long")
head(x_, 50)
x_[is.na(pos_long), pos_long := 0]
x_
y_ = as.xts.data.table(x_[pos_long == 1][, .(date, ret_1)])
charts.PerformanceSummary(y_)
SharpeRatio.annualized(na.omit(y_), scale = 252)


# SUM BUY/SELL ------------------------------------------------------------


