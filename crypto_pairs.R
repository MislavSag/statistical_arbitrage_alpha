library(fastverse)
library(finutils)
library(ggplot2)
library(patchwork)
library(data.table)
library(arrow)
library(roll)
library(lubridate)
library(PerformanceAnalytics)
library(AzureStor)
library(httr)
library(rvest)


# SETUP -------------------------------------------------------------------
# Save path
PATH_SAVE = "H:/strategies/statsarb"
PATH_LEAN = "C:/Users/Mislav/qc_snp/data"
PATH_DATA = "F:/data/equity/us"
if (!dir.exists(PATH_SAVE)) dir.create(PATH_SAVE)

# Parameters
FREQ = "month" # Can be month and quarter


# DATA --------------------------------------------------------------------
# Binance crypto price data from Quantconnect
url = "https://www.quantconnect.com/datasets/binance-crypto-price-data"
p = read_html_live(url)
ticker_table = p |>
  html_element("table.qc-table.table-reflow.ticker-table.hidden-xs") |>
  html_table(fill = TRUE) |>
  as.data.table() |>
  unlist(use.names = FALSE) |>
  na.omit()
ticker_table[grepl("USDT", ticker_table)]
quote_currencies = c(
  "FDUSD", "BUSD", "USDT", "USDC", "TUSD", "DAI",
  "BTC", "BNB", "ETH",
  "TRY", "BRL", "BIDR", "RUB", "AUD", "BKRW", "EUR",
  "GBP", "JPY", "UAH", "NGN", "ZAR", "MXN", "PLN", "ARS", "IDRT"
)
quote_currencies = quote_currencies[order(nchar(quote_currencies), decreasing = TRUE)]
quote_pattern = paste(quote_currencies, collapse = "|")
ticker_main = unique(sub(paste0("(", quote_pattern, ")$"), "", ticker_table))

# Import daily data
prices_raw = crypto(
  path_market_cap = "H:/strategies/cryptotemp/coincodex_marketcap.feather",
  path_prices = "H:/strategies/cryptotemp/binance_spot_1h.feather",
  snapshot_hour = 0,
  n = 200,
  min_constituents = 250,
  first_date = as.Date("2019-01-01")
)
p_raw  = prices_raw$prices
prices = prices_raw$universe
setnames(prices, colnames(prices), tolower(colnames(prices)))

# Check coins available 
tickers_prices = prices[, sort(unique(ticker))]
setdiff(tickers_prices, ticker_main)
tickers_prices[100] %in% ticker_main
setdiff(paste0(tickers_prices, "USDT"), ticker_table)

# Number of coins by date
prices[, .N, by = date] |>
  ggplot(aes(date, N)) +
  geom_line()
p_raw[, .N, by = .(date = as.Date(Datetime))] |>
  ggplot(aes(date, N)) +
  geom_line()
prices[is_index == TRUE, .N, by = date] |>
  ggplot(aes(date, N)) +
  geom_line()

# Keep only universe
prices = prices[is_index == TRUE]

# Create quaterly column
if (FREQ == "quarter") {
  prices[, q := data.table::yearqtr(date)]
} else if (FREQ == "month") {
  prices[, q := data.table::yearmon(date)]
}

# Check duplicates
if (anyDuplicated(prices, by = c("ticker", "date"))) {
  duplicated_symbols = prices[duplicated(prices, by = c("fmp_symbol", "date")), unique(fmp_symbol)]
  paste0(length(duplicated_symbols) / prices[, length(unique(symbol))] * 100, "%")prices = prices[!(fmp_symbol %in% duplicated_symbols)]
}

# Plots
quarters = prices[, sort(unique(q))]
prices[, .N, by = q][order(q)] |>
  ggplot(aes(q, N)) +
  geom_col() +
  scale_x_binned(breaks = quarters)


# PAIRS -------------------------------------------------------------------
# Create pairs for every quarter
pairs_universe = list()
if (FREQ == "month") {
  min_obs = 15
} else if (FREQ == "quarter") {
  min_obs = 40
}
for (i in seq_along(quarters)) {
  # debug
  print(i)

  # data
  q_ = quarters[i]
  dt_ = prices[q == q_]

  # Remove symbols with low number of observations
  keep_symbols = dt_[, .N, by = ticker][N > min_obs, ticker]
  sprintf("Keeping %d symbols out of %d (%.1f%%)", 
          length(keep_symbols), dt_[, length(unique(ticker))],
          100 * length(keep_symbols) / dt_[, length(unique(ticker))])
  dt_ = dt_[ticker %in% keep_symbols]

  # create all possible pairs
  pairs_all = dt_[, unique(ticker)]
  pairs_all = CJ(stock1 = pairs_all, stock2 = pairs_all, unique = TRUE)
  pairs_all = pairs_all[stock1 != stock2]
  pairs_all[, `:=`(first = pmin(stock1, stock2), second = pmax(stock1, stock2))]
  pairs_all = unique(pairs_all, by = c("first", "second"))
  pairs_all[, c("first", "second") := NULL]

  # merge year and save to list
  pairs_all[, q := q_]
  pairs_universe[[i]] = pairs_all
}
pairs_universe = rbindlist(pairs_universe)
setorder(pairs_universe, q)
nrow(pairs_universe) # 395.898.227; 229.238.862; 160.459.565; 504.948.451


# TIME SERIES FEATURES ----------------------------------------------------
# Calculate features
PERIODS = prices[, sort(unique(q))]
prices_sample = prices[, .(ticker, date, close)]
pairs_sample = unique(pairs_universe[, .(stock1, stock2)])
pairs_universe_l = list()
for (i in seq_along(PERIODS)) {
  # Extract year
  # i = 5
  print(i)

  # Choose last quartal
  Q = PERIODS[i]

  # # File name
  # file_name = file.path(PATH_SAVE, "predictors_crypto")
  # if (!dir.exists(file_name)) dir.create(file_name)
  # end_date = zoo::as.Date.yearmon(Q) - 1
  # file_name = file.path(file_name, paste0("predictors_", end_date, ".feather"))
  # if (file.exists(file_name)) next()

  # Spreads
  pairsy = pairs_universe[q == Q]

  # Divide number of rows of pairs universe to 100 chunks
  chunks = split(pairsy, cut(1:nrow(pairsy), breaks = 10))

  # Loop over chunks to calcluate time series features
  pairs_time_series_features_l = list()
  for (j in seq_along(chunks)) {
    # j = 5
    print(j)
    # Prepare chunk
    dt_ = chunks[[j]]
    dt_ = dt_[, .(stock1, stock2)]

    # IMPORTANT: Define start and end date
    end_date   = zoo::as.Date.yearmon(Q) - 1
    start_date = zoo::as.Date.yearmon(Q) - 365 - 30
    dates = prices[date %between% c(start_date, end_date), sort(unique(date))]
    dt_ = dt_[, .(date = dates), by = .(stock1, stock2)]

    # Merge prices with pairs and dates
    dt_ = prices_sample[dt_, on = c("ticker" = "stock1", "date")]
    setnames(dt_, c("ticker", "close"), c("stock1", "close1"))
    dt_ = prices_sample[dt_, on = c("ticker" = "stock2", "date")]
    setnames(dt_, c("ticker", "close"), c("stock2", "close2"))
    dt_ = unique(dt_, by = c("stock1", "stock2", "date"))
    dt_[, ratiospread := close1 / close2]
    dt_[, spreadclose := log(ratiospread)]

    # Calculate Z scores
    dt_[, let(
      sma20  = frollmean(spreadclose, 20, na.rm = TRUE),
      sma60  = frollmean(spreadclose, 60, na.rm = TRUE),
      sma120 = frollmean(spreadclose, 120, na.rm = TRUE),
      sd20   = roll_sd(spreadclose, 20),
      sd60   = roll_sd(spreadclose, 60),
      sd120  = roll_sd(spreadclose, 120)
    ), by = .(stock1, stock2)]
    dt_[, let(
      zscore_20  = (spreadclose - sma20) / sd20,
      zscore_60  = (spreadclose - sma60) / sd60,
      zscore_120 = (spreadclose - sma120) / sd120
    )]

    # Calculate returns
    dt_[, logreturns := spreadclose - shift(spreadclose), by = .(stock1, stock2)]

    # DEBUG
    # dt_[stock1 == "ABB" & stock2 == "ABCB"]
    # dt_[stock1 == "ABB" & stock2 == "ABCB", .(date, logreturns)]
    # plot(as.xts.data.table(dt_[stock1 == "PAA" & stock2 == "PAGP", .(date, logreturns)]))

    # Neg lag zscore - this is the weight we apply
    dt_[, let(
      neg_lagged_zscore_20  = shift(-zscore_20),
      neg_lagged_zscore_60  = shift(-zscore_60),
      neg_lagged_zscore_120 = shift(-zscore_120)
    ), by = .(stock1, stock2)]

    # Calculate the daily returns of the strategy by multiplying the lagged z-score with the daily returns of the spread
    dt_[, let(
      lsr_20 = neg_lagged_zscore_20 * logreturns,
      lsr_60 = neg_lagged_zscore_60 * logreturns,
      lsr_120 = neg_lagged_zscore_120 * logreturns
    )]

    # Create month column
    dt_[, month := ceiling_date(date, "month") - 1]

    # Normalize prices by stock1, stock2 and month
    dt_[, let(
      normalized1 = close1 / data.table::first(close1),
      normalized2 = close2 / data.table::first(close2)
    ), by = .(stock1, stock2, month)]

    # Aggregate to monthly data
    dtm_ = dt_[, .(
      lsr = sum(lsr_20, na.rm = TRUE) / sd(lsr_20, na.rm = TRUE) +
        sum(lsr_60, na.rm = TRUE) / sd(lsr_60, na.rm = TRUE) +
        sum(lsr_120, na.rm = TRUE) / sd(lsr_120, na.rm = TRUE),
      distance = sum((normalized1 - normalized2)^2)
    ), by = .(stock1, stock2, month)]

    # Aggregate to yearly data
    dtq_ = dtm_[, .(
      lsr      = sum(lsr, na.rm = TRUE),
      distance = sum(distance, na.rm = TRUE),
      date     = data.table::last(month)
    ), by = .(stock1, stock2)]
    pairs_time_series_features_l[[j]] = dtq_
  }
  pairs_time_series_features = rbindlist(pairs_time_series_features_l)

  # Ranks
  pairs_time_series_features[, lsr_bucket := frankv(lsr, order = -1L, ties.method = "first") / length(lsr), by = date]
  pairs_time_series_features[, ed_rank := frank(distance, ties.method = "first") / length(distance), by = date]

  # Merge pairs_time_series_features and
  pairs_universe_ = merge(
    pairs_time_series_features, pairs_sample,
    by = c("stock1", "stock2"), all.x = TRUE, all.y = FALSE)

  # Save data for month
  # write_feather(pairs_universe_, file_name)
  pairs_universe_l[[i]] = pairs_universe_
}


# TIME SERIES FEATURES ----------------------------------------------------
# Import data
# paths = list.files(file.path(PATH_SAVE, "predictors_crypto"), full.names = TRUE)
# pairs = lapply(paths, read_feather)
# pairs = rbindlist(pairs)
pairs = rbindlist(pairs_universe_l)

# Check unique dates
pairs[, unique(date)]
pairs[, length(unique(date))] == length(paths[1:12])
pairs[, hist(lsr_bucket)]
pairs[, hist(ed_rank)]

# Define weights for 12 months
width = 12
half_life = 3
lambda = 0.5^(1/half_life)
w = lambda^(width:1)

# Combined scores
setorder(pairs, stock1, stock2, date)
pairs[, roll_wmean_lsr := roll_mean(lsr_bucket, width, weights = w), by = .(stock1, stock2)]
pairs[, roll_wmean_ed  := roll_mean(ed_rank, width, weights = w),  by = .(stock1, stock2)]
pairs[, combo := (roll_wmean_lsr + roll_wmean_ed) / 2]

# Check number of NA's
pairs[, sum(is.na(combo)) / nrow(pairs) * 100]

# Remove missing values
pairs = na.omit(pairs)

# Save
# write_feather(pairs, file.path(PATH_SAVE, "crypto_pairs_combo.feather"))


# RESEARCH ---------------------------------------------------------------
# Add target columns
prices_research = copy(prices)
prices_research[, ret_1 := shift(close, 1L, type = "lead") / close - 1, by = ticker]
prices_research[, ret_5 := shift(close, 5L, type = "lead") / close - 1, by = ticker]
prices_research[, ret_10 := shift(close, 10L, type = "lead") / close - 1, by = ticker]
prices_research[, ret_22 := shift(close, 22L, type = "lead") / close - 1, by = ticker]
prices_research[, ret_66 := shift(close, 66L, type = "lead") / close - 1, by = ticker]
prices_research[, ret_1_o_o := shift(open, 2L, type = "lead") / shift(open, 1L, type = "lead") - 1, by = ticker]
prices_research[, ret_1_o_c := shift(close, 1L, type = "lead") / shift(open, 1L, type = "lead") - 1, by = ticker]
prices_research[, ret_2_o_c := shift(close, 2L, type = "lead") / shift(open, 1L, type = "lead") - 1, by = ticker]

# Descriptive
pairs[, .N, by = date][order(date)] |>
  ggplot(aes(x = date, y = N)) +
  geom_bar(stat = "identity") +
  scale_x_continuous(breaks = seq(0, 1, by = 0.1)) +
  theme_minimal() +
  labs(title = "Number of pairs by quarter", x = "Quantile", y = "Number of pairs")

# Prices to long format to calculate z score more easly
prices_long = prices_research[, .(ticker = toupper(ticker), date, close)]
prices_long = prices_long[ticker %in% pairs[, stock1] | ticker %in% pairs[, stock2]]

# Select n best by var
setorder(pairs, date, combo)
dt = pairs[, .(date, stock1, stock2, combo)]
# 1) Best pairs
dt = dt[, head(.SD, as.integer(nrow(.SD)*0.1)), by = date]
# 2) Worst pairs
# dt = dt[, tail(.SD, as.integer(nrow(.SD)*0.01)), by = date]

# Plot number of pairs through time
dt[, .N, by = date] |>
  _[order(date)] |>
  as.xts.data.table() |>
  plot()

# At least n rows
n_ = 30
keep_tickers = dt[, .(ticker = c(stock1, stock2))] |>
  _[, .N, by = ticker] |>
  _[N >= n_] |>
  _[, ticker]
removed = setdiff(dt[, unique(c(stock1, stock2))], keep_tickers)
print(paste("Removed", length(removed), "tickers with less than 50 occurrences",
            "and", length(keep_tickers), "tickers kept"))


# QC DATA -----------------------------------------------------------------
# Fitler stocks
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
cont = storage_container(bl_endp_key, "qc-backtest")
storage_write_csv(as.data.frame(dt), cont, "pairs_month_crypto.csv")


# EOD PREDICTORS ----------------------------------------------------------
# Calculate Zscore for every quarter-day
dates = dt[, sort(unique(date))]
universe = list()
for (i in seq_along(dates)) {
  # i = 5
  # DEBUG
  print(i)
  if (i == length(dates)) break

  # filter prices we need
  date_ = dates[i]
  date_start_ = date_ - 200
  p = prices_research[date %between% c(date_start_, dates[i+1])]

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

  # 4) Prepare closes for stock1 and stock2 (filter by needed symbols only)
  syms1 = unique(pairs_$stock1)
  syms2 = unique(pairs_$stock2)

  dt_1 = p[ticker %in% syms1, .(date, stock1 = ticker, close_1 = close)]
  dt_2 = p[ticker %in% syms2, .(date, stock2 = ticker, close_2 = close)]

  setkey(dt_1, date, stock1)
  setkey(dt_2, date, stock2)

  # 5) Join closes onto the grid
  res = grid[
    dt_1, on = .(date, stock1)
  ][
    dt_2, on = .(date, stock2)
  ]
  setorder(res, stock1, stock2)

  # alculate Z scores
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
  res[, rel_ret_10 := (close_1 / shift(close_1, 10)) / (close_2 / shift(close_2, 10)) - 1,
      by = .(stock1, stock2)]

  # Filter
  cols_keep_ = c("date", "stock1", "stock2",
                 "zscore", "zscore_20", "zscore_60", "zscore_120",
                 "rel_ret_10")
  res = res[date %between% c(date_, dates[i+1]), ..cols_keep_]
  universe[[i]] = res
}
universe = rbindlist(universe)

# Remove missing values
unvierse = na.omit(universe)

# Prepare data for QC
mean_zscore = rbind(
  universe[, .(date, stock = stock1, zscore, zscore_20, zscore_60, zscore_120)],
  universe[, .(date, stock = stock2, zscore = -zscore, zscore_20 = -zscore_20,
               zscore_60 = -zscore_60, zscore_120 = -zscore_120)]
) |>
  _[, .(
    mean_zscore     = mean(zscore, na.rm = TRUE),
    mean_zscore_20  = mean(zscore_20, na.rm = TRUE),
    mean_zscore_60  = mean(zscore_60, na.rm = TRUE),
    mean_zscore_120 = mean(zscore_120, na.rm = TRUE),
    sum_zscore_bin  = sum(zscore > 0, na.rm = TRUE) - sum(zscore <= 0, na.rm = TRUE),
    wmean_zscore    = weighted.mean(zscore, w = rev(1:length(zscore) / sum(1:length(zscore))), na.rm = TRUE)
  ),by = .(date, stock)]
mean_rel_ret_10 = rbind(
  universe[, .(date, stock = stock1, rel_ret_10)],
  universe[, .(date, stock = stock2, rel_ret_10 = -rel_ret_10)]
)[, .(mean_rel_ret_10 = mean(rel_ret_10, na.rm = TRUE)), by = .(date, stock)]
mean_indicators = mean_zscore[mean_rel_ret_10, on = c("stock", "date")]
mean_indicators = na.omit(mean_indicators)

# Merge prices again
portfolio = prices_research[, .(ticker, date, ret_1, ret_5, ret_10, ret_22)] |>
  _[mean_indicators, on = c(ticker = "stock", "date")] |>
  na.omit()

# Correlations
cor(portfolio[, .(mean_zscore, mean_zscore_20, mean_zscore_60, mean_zscore_120, wmean_zscore)])
portfolio[, cor(mean_zscore, mean_rel_ret_10)]
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
plots_reg("ret_10")
plots_reg("ret_22")

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


# CS BACKTESTING ----------------------------------------------------------
# Params
REBALANCE = "daily" # day, week or month

# Filter data we need for backtest
cols = c("ticker", "date", "ret_1", "close", "open",
         "ret_1_o_o", "ret_1_o_c", "ret_2_o_c")
back = prices_research[, ..cols] |>
  _[mean_indicators, on = c(ticker = "stock", "date")] |>
  na.omit()

# Upsample
# back = back[close_raw > 5]
if (REBALANCE == "week") {
  back[, date := lubridate::ceiling_date(date, unit = "week")]
} else if (REBALANCE == "month") {
  back[, date := data.table::yearmon(date)]
}
if (REBALANCE %in% c("week", "month")) {
  back = back[, .(
    date            = data.table::last(date),
    open            = data.table::first(open),
    close           = data.table::last(close),
    mean_zscore     = data.table::last(mean_zscore),
    mean_rel_ret_10 = data.table::last(mean_rel_ret_10),
    sum_zscore_bin  = data.table::last(sum_zscore_bin)
  ), by = .(ticker, date)]
  back[, combo := ((frank(mean_zscore, ties.method = "first") / length(mean_zscore)) +
                     (frank(mean_rel_ret_10, ties.method = "first")  / length(mean_zscore))) / 2,
       by = date]
  setorder(back, ticker, date)
  back[, ret_1 := close / open - 1]
  back[, ret_1 := shift(ret_1, 1L, type = "lead"), by = ticker]
  back = na.omit(back)
}
back[, hist(mean_zscore)]
setorder(back, date, -mean_zscore)
n = 20
back_long  = back[, tail(.SD, n), by = date]
back_short = back[, head(.SD, n), by = date]
# Long only
if (REBALANCE == "daily") {
  long_xts = back_long[, .(strategy = sum((1/length(ret_1)) * ret_1_o_c)), by = date]
} else {
  long_xts = back_long[, .(strategy = sum((1/length(ret_1)) * ret_1)), by = date]
}
if (REBALANCE == "month") {
  long_xts[, date := zoo::as.yearmon.default(date)]
}
long_xts = as.xts.data.table(long_xts)
charts.PerformanceSummary(long_xts)
scale_ = if (REBALANCE == "week") 52 else if (REBALANCE == "month") 12 else 252
finutils::portfolio_stats(long_xts, scale = scale_)
# Short only
if (REBALANCE == "daily") {
  short_xts = back_short[, .(strategy = sum(-(1/length(ret_1)) * ret_1_o_c)), by = date] |>
    as.xts.data.table()
} else {
  short_xts = back_short[, .(strategy = sum(-(1/length(ret_1)) * ret_1)), by = date] |>
    as.xts.data.table()
}
charts.PerformanceSummary(short_xts)
finutils::portfolio_stats(short_xts, scale = 252)
# Long short
long_short = cbind(long_xts, short_xts)
long_short$strategy_combo = (long_short$strategy/2) + (long_short$`strategy.1` / 2)
charts.PerformanceSummary(long_short)
Return.annualized(long_short, scale = 252)
SharpeRatio.annualized(long_short, scale = 252)
lapply(Drawdowns(long_short), min)
# Save symbols for Quantconnect
if (REBALANCE == "month") {
  qc_data = back_long[, .(month = time_event, date, symbol, weight = 1/10)]
  qc_data[, date_month := zoo::as.Date.yearmon(month, frac = 1)]
} else if (REBALANCE == "week") {
  qc_data = back_long[, .(date, symbol, weight = 1/30)]
}
qc_data = back_long[, .(date, symbol, weight = 1/20)]
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
