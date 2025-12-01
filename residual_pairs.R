library(data.table)
library(finutils)
library(facmodCS)

# TODO: 
# 1) Uncomment market cap coarse

# Save paths
PATH_SAVE = "H:/strategies/statsarb"
PATH_LEAN = "C:/Users/Mislav/qc_snp/data"
PATH_DATA = "F:/data/equity/us"

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


# CHARACTERISTICS ----------------------------------------------------
# 1. Momentum (12-month, skip most recent month)
setorder(prices, symbol, date)
prices[, momentum := shift(close, 21) / shift(close, 252) - 1, by = symbol]

# 2. Low volatility (60-day rolling SD)
prices[, vol_60d := roll_sd(returns, 60), by = symbol]

# 3. Short-term reversal (past month return)
prices[, st_reversal := shift(close, 1) / shift(close, 21) - 1, by = symbol]

# 4. Market returns (SPY) - we already have column spy_returns

# Remove missing values
prices = na.omit(prices)

# Check number of observations by symbol
keep_symbols = prices[, .N > 260, by = symbol]
keep_symbols = keep_symbols[V1 == TRUE, symbol]
paste0("We remove ", length(keep_symbols) / prices[, length(unique(symbol))] - 1, " number of symols")
prices = prices[.(keep_symbols)]


# FACTOR MODEL ESTIMATION -----------------------------------
# Using facmodCS for fundamental factor model
# Following Pelger et al. (2024) "Attention Factors for Statistical Arbitrage"
# 4 factors: Market, Momentum, Volatility, Short-term Reversal

# IMPORTANT: All exposures must be LAGGED (t-1) to predict returns at time t
# Otherwise we have look-ahead bias!

# Prepare data for factor model
dt_factor = prices[, .(
  date,
  symbol,
  returns,           # Target variable (current return at time t)
  spy_returns_lag = shift(spy_returns, 1),    # Lagged market factor
  momentum_lag = shift(momentum, 1),          # Lagged 12-month momentum  
  vol_60d_lag = shift(vol_60d, 1),            # Lagged 60-day volatility
  st_reversal_lag = shift(st_reversal, 1)     # Lagged short-term reversal
), by = symbol]

# Remove NA from lagging
dt_factor = na.omit(dt_factor)

# Rank transform LAGGED characteristics for robustness (uniformization + Gaussianization)
# Keep market returns raw (constant per date, can't rank cross-sectionally)
dt_factor[, `:=`(
  spy_returns_lag_raw = spy_returns_lag,  # Keep market raw (not ranked)
  momentum_rank = qnorm(frank(momentum_lag) / (.N + 1)),
  vol_rank = qnorm(frank(vol_60d_lag) / (.N + 1)),
  reversal_rank = qnorm(frank(st_reversal_lag) / (.N + 1))
), by = date]

# Check for extreme stock returns (likely data errors)
extreme_returns = dt_factor[abs(returns) > 1]  # >50% daily return
if (nrow(extreme_returns) > 0) {
  print(head(extreme_returns[order(-abs(returns)), .(date, symbol, returns)], 20))
  bad_symbols = extreme_returns[, unique(symbol)]
  dt_factor = dt_factor[!symbol %in% bad_symbols]
}

# Estimate factor model with rolling windows
# Returns: idiosyncratic returns + covariance matrix
estimate_factor_model = function(dt, window = 252, min_obs = 50) {
  # dt = copy(dt_factor)

  # get all dates
  all_dates = dt[, sort(unique(date))]
  results = list()

  # loop over dates - 
  for (i in window:length(all_dates)) {
    # i = 300
    
    if (i %% 100 == 0) cat("Processing date", i, "of", length(all_dates), "\n")
    
    # Get window dates
    window_dates = all_dates[(i - window + 1):i]
    
    # Balance panel for this window (required by fitFfm)
    dt_window = dt[date %in% window_dates]
    symbols_all_dates = dt_window[, .N, by = symbol][N == window, symbol]
    dt_balanced = dt_window[symbol %in% symbols_all_dates]
    
    n_symbols = dt_balanced[, uniqueN(symbol)]
    if (n_symbols < min_obs) next
    
    # Convert to data.frame (required by facmodCS)
    df = as.data.frame(dt_balanced)
    
    # Fit fundamental factor model (Fama-MacBeth cross-sectional regression)
    # Returns(t) ~ spy_returns(t-1) + momentum(t-1) + vol(t-1) + reversal(t-1)
    fit = tryCatch({
      facmodCS::fitFfm(
        data = df,
        asset.var = "symbol",
        ret.var = "returns",
        date.var = "date",
        exposure.vars = c("spy_returns_lag_raw", "momentum_rank", "vol_rank", "reversal_rank"),
        fit.method = "LS",              # Least squares (Fama-MacBeth)
        full.resid.cov = TRUE           # Calculate idiosyncratic covariance
        # Note: resid.scaleType and lambda only work with WLS or W-Rob, not LS
      )
    }, error = function(e) {
      cat("Error at date", as.character(all_dates[i]), ":", e$message, "\n")
      return(NULL)
    })
    
    if (is.null(fit)) next
    
    # Store results
    current_date = all_dates[i]
    
    results[[as.character(current_date)]] = list(
      date = current_date,
      # Idiosyncratic returns (factor-neutral residuals)  
      idio_returns = data.table(
        symbol = names(fit$residuals),
        date = current_date,
        idio_return = as.numeric(fit$residuals)
      ),
      # Factor premiums (cross-sectional factor returns)
      factor_returns = fit$factor.returns,
      # Note: fit$resid.cov is NULL - we'll calculate idio covariance later
      # from the time series of idiosyncratic returns when needed for pairs
      # Number of assets in this estimation
      n_assets = length(fit$residuals)
    )
  }
  
  cat("Completed! Generated", length(results), "factor model estimations.\n")
  return(results)
}

# Run estimation
factor_results = estimate_factor_model(
  dt_factor[date %between% c(as.Date("2020-01-01"), as.Date("2024-01-01"))], 
  window = 252, 
  min_obs = 50)

# Extract idiosyncratic returns
idio_returns = rbindlist(lapply(factor_results, function(x) x$idio_returns))

# Merge idiosyncratic returns back to main prices table
prices = prices[idio_returns, on = c(fmp_symbol = "symbol", "date")]

# Summary statistics
cat("\n=== IDIOSYNCRATIC RETURNS SUMMARY ===\n")
cat("Total observations:", nrow(idio_returns), "\n")
cat("Unique symbols:", idio_returns[, uniqueN(symbol)], "\n")
cat("Date range:", as.character(range(idio_returns$date)), "\n")
cat("Mean idio return:", round(mean(idio_returns$idio_return, na.rm = TRUE), 6), "\n")
cat("SD idio return:", round(sd(idio_returns$idio_return, na.rm = TRUE), 6), "\n")



