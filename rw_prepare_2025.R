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
PATH_SAVE = "D:/strategies/statsarb"
PATH_LEAN = "C:/Users/Mislav/qc_snp/data"
PATH_DATA = "F:/data/equity/us"


# DATA --------------------------------------------------------------------
# Import daily data
prices = coarse(
  min_mean_mon_price = 5,
  min_mean_mon_volume = 100000,
  dollar_vol_n = 3000,
  file_path = file.path(PATH_LEAN, "all_stocks_daily"),
  min_obs = 2 * 252,
  price_threshold = 1e-008,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  etfs = FALSE,
  profiles_fmp = TRUE,
  fmp_api_key = "6c390b6279b8a67cc7f63cbecbd69430",
  min_last_mon_mcap = file.path(PATH_DATA, "fundamentals", "market_cap.parquet")
)

# Create quaterly column
prices[, q := data.table::yearqtr(date)]

# Plots
quarters = prices[, sort(unique(q))]
prices[, .N, by = q][order(q)] |>
  ggplot(aes(q, N)) +
  geom_col() +
  scale_x_binned(breaks = quarters)

# Metadata for every stock
profile = unique(prices[, .(fmp_symbol, industry, sector, isEtf, isFund)])
profile_cols = c("symbol", "isin", "companyName", "currency", "exchange", "country")
profile_meta = unique(prices[, .(fmp_symbol, industry, sector, isEtf, isFund,
                                 isin, currency, exchange, country)])


# PAIRS -------------------------------------------------------------------
# Create pairs for every quarter
pairs_universe = list()
for (i in seq_along(quarters)) {
  print(i)
  # data
  q_ = quarters[i]
  dt_ = prices[q == q_]

  # Remove symbols with low number of observations
  keep_symbols = dt_[, .N, by = symbol][N > 40, symbol]
  sprintf("Keeping %d symbols out of %d (%.1f%%)", length(keep_symbols), dt_[, length(unique(symbol))],
          100 * length(keep_symbols) / dt_[, length(unique(symbol))])
  dt_ = dt_[symbol %in% keep_symbols]

  # create all possible pairs
  pairs_all = dt_[, unique(fmp_symbol)]
  pairs_all = CJ(stock1 = pairs_all, stock2 = pairs_all, unique = TRUE)
  pairs_all = pairs_all[stock1 != stock2]
  pairs_all[, `:=`(first = pmin(stock1, stock2), second = pmax(stock1, stock2))]
  pairs_all = unique(pairs_all, by = c("first", "second"))
  pairs_all[, c("first", "second") := NULL]

  # Merge industries and sectors for each stock in the pair
  pairs_all = merge(pairs_all, profile, by.x = "stock1", by.y = "fmp_symbol")
  cols = c("sector", "industry", "isEtf", "isFund")
  setnames(pairs_all, cols, paste0(cols, "_stock1"))
  pairs_all = merge(pairs_all, profile, by.x = "stock2", by.y = "fmp_symbol")
  setnames(pairs_all, cols, paste0(cols, "_stock2"))
  pairs_all[, same_sector := 0]
  pairs_all[sector_stock1 == sector_stock2, same_sector := 1]
  # pairs_all = merge(pairs_all, profile, by.x = "stock2", by.y = "fmp_symbol")
  pairs_all[, same_industry := 0]
  pairs_all[industry_stock1 == industry_stock2, same_industry := 1]
  pairs_all = pairs_all[, .(stock1, stock2, same_sector, same_industry)]

  # merge year and save to list
  pairs_all[, q := q_]
  pairs_universe[[i]] = pairs_all
}
pairs_universe = rbindlist(pairs_universe)
setorder(pairs_universe, q)
nrow(pairs_universe) # 395.898.227


# TIME SERIES FEATURES ----------------------------------------------------
# TODO: Choose year, but later maybe expand for all years
# YEARS = 2020:2025
STARTY   = 2020
QUARTERS = prices[q >= STARTY, sort(unique(q))]
pairs_universe_l = list()
for (i in seq_along(QUARTERS)) {
  # Extract year
  # i = 5
  print(i)

  # Choose last quartal
  Q = QUARTERS[i]

  # Extract last 4 quarters
  # qs = QUARTERS[(i - 5):(i - 1)]

  # Spreads
  pairsy = pairs_universe[q == Q]

  # DEBUG
  # pairsy[stock1 == "PAA" & stock2 == "PAGP"]
  # pairsy[stock1 == "BEP" & stock2 == "BEPC"]
  # pairsy[stock1 == "MCO" & stock2 == "SPGI"]
  # pairsy[stock1 == "ASB" & stock2 == "HBAN"]
  # pairsy[stock1 == "KNX" & stock2 == "WERN"]
  # chunks = pairsy[(stock1 == "PAA" & stock2 == "PAGP") |
  #                   (stock1 == "BEP" & stock2 == "BEPC")]
  # dt_ = copy(chunks)

  # Divide number of rows of pairs universe to 100 chunks
  chunks = split(pairsy, cut(1:nrow(pairsy), breaks = 75))

  # Loop over chunks to calcluate time series features
  pairs_time_series_features_l = list()
  for (j in seq_along(chunks)) {
    # j = 1
    print(j)
    # Prepare chunk
    dt_ = chunks[[j]]
    dt_ = dt_[, .(stock1, stock2)]
    start_date = zoo::as.Date.yearqtr(Q-0.25) - 365 - 30
    end_date   = zoo::as.Date.yearqtr(Q-0.25)
    dates = prices[date %between% c(start_date, end_date), sort(unique(date))]
    dt_ = dt_[, .(date = dates), by = .(stock1, stock2)]

    # Merge prices with pairs and dates
    dt_ = prices[, .(fmp_symbol, date, close)][dt_, on = c("fmp_symbol" = "stock1", "date")]
    setnames(dt_, c("fmp_symbol", "close"), c("stock1", "close1"))
    dt_ = prices[, .(fmp_symbol, date, close)][dt_, on = c("fmp_symbol" = "stock2", "date")]
    setnames(dt_, c("fmp_symbol", "close"), c("stock2", "close2"))
    dt_ = unique(dt_, by = c("stock1", "stock2", "date"))
    dt_[, ratiospread := close1 / close2]
    dt_[, spreadclose := log(ratiospread)]

    # Calculate Z scores
    dt_[, sma20  := frollmean(spreadclose, 20, na.rm = TRUE), by = .(stock1, stock2)]
    dt_[, sd20   := roll_sd(spreadclose, 20), by = .(stock1, stock2)]
    dt_[, zscore := (spreadclose - sma20) / sd20]

    # Calculate returns
    dt_[, logreturns := spreadclose - shift(spreadclose), by = .(stock1, stock1)]

    # DEBUG
    # dt_[stock1 == "ABB" & stock2 == "ABCB"]
    # dt_[stock1 == "ABB" & stock2 == "ABCB", .(date, logreturns)]
    # plot(as.xts.data.table(dt_[stock1 == "PAA" & stock2 == "PAGP", .(date, logreturns)]))

    # Neg lag zscore - this is the weight we apply
    dt_[, neg_lagged_zscore := shift(-zscore), by = .(stock1, stock2)]

    # Calculate the daily returns of the strategy by multiplying the lagged z-score with the daily returns of the spread
    dt_[, lsr := neg_lagged_zscore * logreturns]

    # Create month columnd222.138
    dt_[, month := ceiling_date(date, "month") - 1]

    # Normalize prices by stock1, stock2 and month
    dt_[, let(
      normalized1 = close1 / data.table::first(close1),
      normalized2 = close2 / data.table::first(close2)
    ), by = .(stock1, stock2, month)]

    # Aggregate to monthly data
    dtm_ = dt_[, .(lsr = sum(lsr, na.rm = TRUE) / sd(lsr, na.rm = TRUE),
                   distance = sum((normalized1 - normalized2)^2)),
               by = .(stock1, stock2, month)]

    # Aggregate to yearly data
    dtq_ = dtm_[, .(lsr = sum(lsr, na.rm = TRUE),
                    distance = sum(distance, na.rm = TRUE)),
                by = .(stock1, stock2)]
    dtq_[, q := Q - 0.25]
    pairs_time_series_features_l[[j]] = dtq_
  }
  pairs_time_series_features = rbindlist(pairs_time_series_features_l)

  # Ranks
  pairs_time_series_features[, lsr_rank := frankv(lsr, order = -1L, ties.method = "first"), by = q]
  pairs_time_series_features[, ed_rank := frank(distance, ties.method = "first"), by = q]

  # Quantile bucketize
  # pairs_time_series_features[, lsr_bucket := cut(lsr, breaks = quantile(lsr, probs = 0:100/100), labels = 1:100, right = FALSE), by = y]
  # pairs_time_series_features[, ed_bucket := cut(-distance, breaks = quantile(-distance, probs = 0:100/100), labels = 1:100, right = FALSE), by = y]
  # pairs_time_series_features[, names(.SD) := lapply(.SD, as.integer), .SDcols = c("lsr_bucket", "ed_bucket")]
  pairs_time_series_features[, lsr_bucket := .bincode(lsr, breaks = quantile(pairs_time_series_features$lsr, probs = 0:100/100)), by = q]
  pairs_time_series_features[, ed_bucket  := .bincode(-distance, breaks = quantile(-distance, probs = 0:100/100)), by = q]

  # DEBUG
  # pairs_time_series_features[stock1 == "PAA" & stock2 == "PAGP"]
  # pairs_time_series_features[stock1 == "PAGP" & stock2 == "PAA"]
  # pairs_time_series_features[stock1 == "BEP" & stock2 == "BEPC"]
  # pairs_time_series_features[stock1 == "BEPC" & stock2 == "BEP"]

  # Merge pairs_time_series_features and
  pairs_universe_ = merge(pairs_time_series_features, pairs_universe,
                          by = c("stock1", "stock2", "q"), all.x = TRUE, all.y = FALSE)
  pairs_universe_l[[i]] = na.omit(pairs_universe_)
}

# Merge results for all pairs
pairs_features = rbindlist(pairs_universe_l)
fwrite(pairs_features, file.path(PATH_SAVE, "pairs_features.csv"))

# Combined scores
setorder(pairs_features, stock1, stock2, q)
# Give more weigts to newer data
test = pairs_features[1:1000] |>
  _[, .(lsr_bucket, ed_bucket, q, (roll::roll_mean(lsr_bucket, 8, weights = 1:8, min_obs = 8) +
                                     roll::roll_mean(ed_bucket, 8, weights = 1:8, min_obs = 8)) / 2), by = .(stock1, stock2)]
head(test, 20)
tail(test, 20)
pairs_features[, combo_score := (roll::roll_mean(lsr_bucket, 8, weights = 1:8, min_obs = 8) +
                                   roll::roll_mean(ed_bucket, 8, weights = 1:8, min_obs = 8)) / 2,
               by = .(stock1, stock2)]

# Check number of NA's
pairs_features[, sort(unique((q)))]
pairs_features[, sum(is.na(combo_score)) / nrow(pairs_features) * 100]
pairs_features[q > 2023.5][, sum(is.na(combo_score)) / nrow(pairs_features) * 100]

# Convert to wide format by year
pairs_combo = dcast(pairs_features, stock1 + stock2 + same_sector + same_industry ~ q * 100, value.var = "combo_score")

# Add data from profiles
pairs_combo = merge(pairs_combo, profile_meta, by.x = "stock1",
                    by.y = "fmp_symbol", all.x = TRUE, all.y = FALSE)
cols = colnames(profile_meta)[-1]
setnames(pairs_combo, cols, paste0(cols, "1"))
pairs_combo = merge(pairs_combo, profile_meta, by.x = "stock2",
                    by.y = "fmp_symbol", all.x = TRUE, all.y = FALSE)
setnames(pairs_combo, cols, paste0(cols, "2"))

# Save
fwrite(pairs_combo, file.path(PATH_SAVE, "pairs_combo.csv"))
