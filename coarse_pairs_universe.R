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
if (!dir.exists(PATH_SAVE)) dir.create(PATH_SAVE)

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
paste0(length(duplicated_symbols) / prices[, length(unique(symbol))] * 100, "%")prices = prices[!(fmp_symbol %in% duplicated_symbols)]

# Plots
quarters = prices[, sort(unique(q))]
prices[, .N, by = q][order(q)] |>
  ggplot(aes(q, N)) +
  geom_col() +
  scale_x_binned(breaks = quarters)

# Metadata for every stock
profile = read_parquet("F:/data/equity/us/fundamentals/prfiles.parquet")
profile_meta = profile[, .(fmp_symbol = symbol, industry, sector, isEtf, isFund,
                           isin, currency, exchange, country)]
profile = unique(profile[, .(fmp_symbol = symbol, industry, sector, isEtf, isFund, companyName)])


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
  keep_symbols = dt_[, .N, by = symbol][N > min_obs, symbol]
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
nrow(pairs_universe) # 395.898.227; 229.238.862; 160.459.565; 504.948.451

# TODO
# filter by same sector here so we have lower numbe of symbols in next step
# pairs_all = pairs_all[same_sector == 1L & same_industry == 1L]

# Save
file_ = file.path(PATH_SAVE, "pairs.feather")
write_feather(pairs_universe, file_)
