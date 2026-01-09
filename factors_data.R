library(frenchdata)
library(data.table)
library(tidyfinance)
library(finutils)


# Config
PATH_SAVE = "H:/strategies/statsarb"
PATH_LEAN = "C:/Users/Mislav/qc_snp/data"
PATH_DATA = "F:/data/equity/us"

# Parameters
START_DATE = "1962-01-01"
END_DATE   = "2025-10-01"

# FF meta
ff_meta = as.data.table(list_supported_types())
ff_meta[grepl("Reversal.*Daily", dataset_name)]
ff_meta[grepl("Mom.*Daily", dataset_name)]
ff_meta[grepl("q5", type)]

# Get Famma-Fench factors
ff5       = as.data.table(download_data("factors_ff_5_2x3_daily", "1962-01-01", "2025-10-01"))
ff_st_rev = as.data.table(download_data("factors_ff_shortterm_reversal_factor_st_rev_daily", "1962-01-01", "2025-10-01"))
ff_lt_rev = as.data.table(download_data("factors_ff_longterm_reversal_factor_lt_rev_daily", "1962-01-01", "2025-10-01"))
ff_mom    = as.data.table(download_data("factors_ff_momentum_factor_daily", "1962-01-01", "2025-10-01"))
factors = Reduce(
  function(x, y) merge(x, y, by = "date"), 
  list(ff5, ff_st_rev, ff_lt_rev, ff_mom))
rf = factors[, .(date, risk_free)]
factors = factors[, .SD, .SDcols = -"risk_free"]
setkey(factors, date)

# Import daily data
prices = coarse(
  min_mean_mon_price = 5,
  min_mean_mon_volume = 200000,
  min_last_mon_mcap = 1e8,
  dollar_vol_n = 4000,
  file_path = file.path(PATH_LEAN, "all_stocks_daily"),
  # market_cap_fmp_file = file.path(PATH_DATA, "fundamentals", "market_cap.parquet"),
  min_obs = 2 * 252,
  price_threshold = 1e-008,
  duplicates = "fast",
  add_dv_rank = FALSE,
  add_day_of_month = FALSE,
  etfs = FALSE,
  market_symbol = "spy",
  profiles_fmp_file = "F:/data/equity/us/fundamentals/prfiles.parquet"
)

# Check duplicates
if (anyDuplicated(prices, by = c("fmp_symbol", "date"))) {
  duplicated_symbols = prices[duplicated(prices, by = c("fmp_symbol", "date")), unique(fmp_symbol)]
  paste0(length(duplicated_symbols) / prices[, length(unique(symbol))] * 100, "%")
  prices = prices[fmp_symbol %notin% duplicated_symbols]
  anyDuplicated(prices, by = c("fmp_symbol", "date"))
}

# Returns in wide format
ret_wide = dcast(prices, date ~ symbol, value.var = "returns", fill = NA)
setkey(ret_wide, date)
ret_wide[, 1:6]

# Align returns and factors
common_dates = as.Date(intersect(ret_wide[, date], factors[, date]))
ret_wide = ret_wide[date %in% common_dates]
factors  = factors[date %in% common_dates]

# Convert to pure matrices for linear algebra
# Remove date column for the matrix math
Y_full = as.matrix() 
X_full = as.matrix(factors[, -1])
dates  = sort(ret_wide$date)

estimate_daily_oos_residuals <- function(
  returns_wide,                    # data.table with columns: date, sid, returns
  ffactors,                    # data.table with columns: date, factor columns
  initial_oos_year = 2000,
  size_window = 252,         # rolling window size (trading days)
  n_factors = 8,             # number of FF factors to use
  min_obs_pct = 1.0          # require 100% non-missing in window
) {
  # Debug
  # returns_wide = copy(ret_wide)
  # ffactors = copy(factors)
  # initial_oos_year = 2000
  # size_window = 252
  # n_factors = 8
  # min_obs_pct = 1.0

  # Convert to matrix
  dates = returns_wide[, sort(date)]
  returns_mat = as.matrix(returns_wide[, -1])
  factors_mat = as.matrix(factors[, -1])

  # Select n factors
  if (ncol(factors_mat) > n_factors) {
    factors_mat = factors_mat[, 1:n_factors, drop = FALSE]
  }
  
  # Find first OOS date
  first_oos_idx = which(data.table::year(dates) >= initial_oos_year)[1]
  cat("First OOS date:", as.character(dates[first_oos_idx]), "\n")
  
  # help vars
  T_ = nrow(returns_mat)
  N_ = ncol(returns_mat)
  
  # Initialize output
  residuals_oos = matrix(0, nrow = T_ - first_oos_idx + 1, ncol = N)
  
  # Rolling window estimation
  cat("Estimating residuals...\n")
  pb = txtProgressBar(min = 0, max = T_ - first_oos_idx + 1, style = 3)
  
  for (t in seq_len(T_ - first_oos_idx + 1)) {
    # debug
    # t = 1

    # start index
    idx = first_oos_idx + t - 1
    
    # Skip if not enough history
    if (idx <= size_window) {
      setTxtProgressBar(pb, t)
      next
    }
    
    # Get training window
    train_idx = (idx - size_window):(idx - 1)
    Y_train = returns_mat[train_idx, , drop = FALSE]
    X_train = factors_mat[train_idx, , drop = FALSE]
    
    # Check for missing values in training window
    has_enough_data <- colSums(!is.na(Y_train)) >= (size_window * min_obs_pct)
    
    if (n_factors == 0) {
      # No factor model - just use raw returns
      residuals_oos[t, has_enough_data] <- returns_mat[idx, has_enough_data]
    } else {
      # For each asset with enough data, fit factor model
      for (j in which(has_enough_data)) {
        # j = 8370
        y = Y_train[, j]
        
        # Skip if any missing in training or test
        if (any(is.na(y)) || is.na(returns_mat[idx, j])) next
        
        # Fit linear regression (no intercept)
        fit = lm.fit(X_train, y)
        loadings = coef(fit)
        
        # Compute OOS residual
        oos_return = returns_mat[idx, j]
        factor_return = sum(factors_mat[idx, ] * loadings)
        residuals_oos[t, j] <- oos_return - factor_return
      }
    }
    
    setTxtProgressBar(pb, t)
  }
  close(pb)
  
  # Convert back to long format
  oos_dates = dates[first_oos_idx:T_]
  sids = colnames(returns_wide)
  anyDuplicated(sids)
  
  length(rep(oos_dates, each = N))
  # 76.293.756
  length(rep(sids, times = length(oos_dates)))
  # 76.300.232
  length(as.vector(t(residuals_oos)))
  # 76.293.756

  residuals_dt = data.table(
    date = rep(oos_dates, each = N),
    sid = rep(sids, times = length(oos_dates)),
    residual = as.vector(t(residuals_oos))
  )
  
  # Remove zeros (missing data)
  residuals_dt = residuals_dt[residual != 0]
  
  return(residuals_dt)
}



# # FF files used in Deep learning arbitrage paper
# FAMA_FRENCH_FACTORS_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"
# FAMA_FRENCH_5_FACTORS_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"
# FAMA_FRENCH_MOM_FACTOR_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip"
# FAMA_FRENCH_LT_REV_FACTOR_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_ST_Reversal_Factor_daily_CSV.zip"
# FAMA_FRENCH_ST_REV_FACTOR_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_LT_Reversal_Factor_daily_CSV.zip"

# # Optinally, dont use for now: q factor model
# q_factors = download_data(
#   type = "factors_q5_daily", 
#   start_date = "1962-01-01", 
#   end_date = "2025-10-01"
# )
# setDT(q_factors)

# # Optinally, dont use for now: Goyal macroecnomic data
# goyal_macro = download_data(
#   type = "macro_predictors_monthly",
#   start_date = START_DATE,
#   end_date = END_DATE
# )
# setDT(goyal_macro)


# Input podaci
# monthly_characteristics_csv_path
# yy 	mm 	date 	permno 	ret 	A2ME 	AC 	AT 	ATO 	BEME 	... 	Resid_Var 	RNA 	ROA 	ROE 	S2P 	SGA2S 	Spread 	ST_REV 	SUV 	Variance 
#       	0 	1963 	1 	19630131 	10006 	0.047002 	2.622475 	0.057272 	251.20 	-71.433333 	1.403588 	... 	0.000089 	-5.200000 	0.034922 	0.049448 	2.237247 	0.093980 	0.010138 	0.058319 	-0.363080 	0.000109 
#       	1 	1963 	1 	19630131 	10014 	0.034483 	1.689561 	-0.069509 	24.63 	6.632194 	0.888203 	... 	0.001497 	0.409173 	0.046261 	0.086458 	2.023632 	0.134915 	0.041162 	0.074074 	0.657892 	0.001

# optionaly normalizes 
# if normalize_characteristics:
#     char_data = data.iloc[:,char_indices]
#     grouped = char_data.groupby("date")
#     char_data_normalized = grouped.transform(
#         lambda x: x.rank(method="first") / x.count() - 0.5
#     )
#     data.iloc[:, 5:] = char_data_normalized
# >>> data_array.shape
# (648, 9483, 47)
# >>> data.shape
# (1232680, 47)

# Daily returns
# >>> rf_rates
#   Mkt-RF 	SMB 	HML 	RF 
# Date 	 	 	 	 
# 19260701 	0.09 	-0.25 	-0.27 	0.01 
# 19260702 	0.45 	-0.33 	-0.06 	0.01 
# >>> risk_free_rates
# array([[0.0001],
#        [0.0001],
#        [0.0001],
#        ...,
#        [0.    ],
#        [0.    ],
#        [0.    ]]) 