library(fastverse)
library(finutils)
library(ggplot2)
library(arrow)
library(httr)
library(roll)
library(lubridate)
library(AzureStor)


# SETUP -------------------------------------------------------------------
# Save path
PATH_SAVE = "H:/strategies/statsarb"
PATH_LEAN = "C:/Users/Mislav/qc_snp/data"
PATH_DATA = "F:/data/equity/us"

# Parameters
FREQ = "month" # Can be month and quarter


# DATA --------------------------------------------------------------------
# Import daily data
prices = coarse(
  min_mean_mon_price = 5,
  min_mean_mon_volume = 200000,
  dollar_vol_n = 4000,
  file_path = file.path(PATH_LEAN, "all_stocks_daily"),
  min_obs = 2 * 252,
  price_threshold = 1e-008,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  etfs = FALSE,
  profiles_fmp_file = "F:/data/equity/us/fundamentals/prfiles.parquet",
  min_last_mon_mcap = file.path(PATH_DATA, "fundamentals", "market_cap.parquet")
)

# Create quaterly column
if (FREQ == "quarter") {
  prices[, q := data.table::yearqtr(date)]
} else if (FREQ == "month") {
  prices[, q := data.table::yearmon(date)]
}

# Check duplicates
anyDuplicated(prices, by = c("fmp_symbol", "date"))
duplicated_symbols = prices[duplicated(prices, by = c("fmp_symbol", "date")), unique(fmp_symbol)]
paste0(length(duplicated_symbols) / prices[, length(unique(symbol))] * 100, "%")
prices = prices[symbol %notin% duplicated_symbols]

# Import pairs
file_ = file.path(PATH_SAVE, "pairs.feather")
pairs = read_feather(file_)
setkey(pairs, q)


# TIME SERIES FEATURES ----------------------------------------------------
# Comments
# filtering columns in prices object are calcualted on end of the month.
# possible pairs are calculated at current month
#

# Calculate features
STARTY  = 2017
PERIODS = prices[q >= STARTY, sort(unique(q))]
prices_sample = prices[, .(fmp_symbol, date, close)]
pairs_sample = unique(pairs[, .(stock1, stock2, same_sector, same_industry)])
pairs_universe_l = list()
for (i in seq_along(PERIODS)) {
  # Extract year
  # i = 5
  print(i)

  # Choose last quartal
  Q = PERIODS[i]

  # File name
  file_name = file.path(PATH_SAVE, "predictors")
  if (!dir.exists(file_name)) dir.create(file_name)
  end_date = zoo::as.Date.yearmon(Q) - 1
  file_name = file.path(file_name, paste0("predictors_", end_date, ".feather"))
  if (file.exists(file_name)) next()

  # Spreads
  pairsy = pairs[.(Q)]

  # Divide number of rows of pairs universe to 100 chunks
  chunks = split(pairsy, cut(1:nrow(pairsy), breaks = 50))

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
    dt_ = prices_sample[dt_, on = c("fmp_symbol" = "stock1", "date")]
    setnames(dt_, c("fmp_symbol", "close"), c("stock1", "close1"))
    dt_ = prices_sample[dt_, on = c("fmp_symbol" = "stock2", "date")]
    setnames(dt_, c("fmp_symbol", "close"), c("stock2", "close2"))
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
  write_feather(pairs_universe_, file_name)
}
