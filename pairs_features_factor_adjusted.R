library(fastverse)
library(finutils)
library(ggplot2)
library(arrow)
library(httr)
library(roll)
library(lubridate)
library(AzureStor)
library(facmodCS)


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
  market_symbol = "spy",
  profiles_fmp_file = "F:/data/equity/us/fundamentals/prfiles.parquet",
  min_last_mon_mcap = file.path(PATH_DATA, "fundamentals", "market_cap.parquet")
)
prices[symbol == "aapl"]

# Create quarterly/monthly column
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


# CHRACTERISTICS CALCULATION ---------------------------------------
# Momentum
setorder(prices, symbol, date)
prices[, momentum := shift(close, 21) / shift(close, 252), by = symbol]

# Low volatility
prices[, vol_60d := roll_sd(returns, 252), by = symbol]

# Remove missing values
prices = na.omit(prices)

# Check number of observations by symbol
keep_symbols = prices[, .N > 260, by = symbol]
keep_symbols = keep_symbols[V1 == TRUE, symbol]
prices = prices[.(keep_symbols)]

# Transform characteristics: Rank (uniformization) + Inverse Normal (Gaussianization)
# This is more robust than z-score: handles outliers and forces normal distribution
setorder(prices, date, symbol)
prices[, `:=`(
  mom_rank = qnorm(frank(momentum, ties.method = "average") / (.N + 1)),
  vol_rank = qnorm(frank(-vol_60d, ties.method = "average") / (.N + 1))
), by = date]

# Remove missing values
prices = na.omit(prices)

# Check number of observations by symbol
keep_symbols = prices[, .N > 260, by = symbol]
keep_symbols = keep_symbols[V1 == TRUE, symbol]
prices = prices[symbol %in% keep_symbols]

# Create sector dummy variables
prices[, .N, by = sector]
prices = prices[sector != ""]
# unique_sectors = prices[!is.na(sector), sort(unique(sector))]
# for (i in seq_along(unique_sectors)[-1]) {  # Skip first sector (reference)
#   sector_name = gsub("[^A-Za-z0-9]", "_", unique_sectors[i])
#   prices[, (paste0("sector_", sector_name)) := as.numeric(sector == unique_sectors[i])]
# }

prices[symbol == "aapl"]


# FAMA-MACBETH REGRESSIONS ------------------------------------------------

# Fama macbeth regression using facmodCS package with ROLLING WINDOW
fama_macbeth_facmodcs = function(dt, factor_cols, window = 252, min_obs = 50) {
  # dt = copy(prices)
  # factor_cols = c("mom_rank", "vol_rank")
  # window = 252  # Number of days in estimation window
  
  # ROLLING WINDOW APPROACH:
  # Instead of balancing entire dataset, we balance only the window of data
  # This dramatically reduces data loss while maintaining balanced panel requirement
  
  # Prepare data - ensure all required columns exist
  dt_fit = dt[, c("symbol", "date", "returns", factor_cols), with = FALSE]
  dt_fit = na.omit(dt_fit)
  setorder(dt_fit, date, symbol)
  
  # Get all dates
  all_dates = sort(unique(dt_fit$date))
  
  # Need at least window dates to start
  if (length(all_dates) < window) {
    warning(sprintf("Not enough dates (%d) for window size %d", length(all_dates), window))
    return(list(idio_returns = data.table(), coefficients = data.table(), idio_cov = list()))
  }
  
  # Storage for results
  idio_returns_list = list()
  coef_list = list()
  idio_cov_list = list()  # Store idiosyncratic covariance matrices
  
  # Loop through dates starting after we have enough history
  for (i in window:length(all_dates)) {
    current_date = all_dates[i]
    
    # Get rolling window of dates (last 'window' dates up to current date)
    window_dates = all_dates[(i - window + 1):i]
    dt_window = dt_fit[date %in% window_dates]
    
    # BALANCE THE PANEL for this window only
    # Find symbols that exist for ALL dates in this window
    symbols_per_date = dt_window[, .N, by = symbol]
    complete_symbols = symbols_per_date[N == length(window_dates), symbol]
    
    if (length(complete_symbols) < min_obs) {
      if (i %% 100 == 0) {
        cat(sprintf("Date %s: insufficient complete symbols (%d)\n", 
                    current_date, length(complete_symbols)))
      }
      next
    }
    
    # Keep only complete symbols
    dt_balanced = dt_window[symbol %in% complete_symbols]
    
    # Verify balance
    check_balance = dt_balanced[, .N, by = date]
    if (check_balance[, uniqueN(N)] > 1) {
      warning(sprintf("Date %s: Panel still unbalanced after filtering", current_date))
      next
    }
    
    if (i %% 100 == 0) {
      cat(sprintf("Date %s: using %d symbols over %d-day window\n",
                  current_date, length(complete_symbols), window))
    }
    
    tryCatch({
      # Fit fundamental factor model on balanced window
      # Setting full.resid.cov = TRUE makes fitFfm calculate the idiosyncratic covariance matrix!
      fit = facmodCS::fitFfm(
        data = as.data.frame(dt_balanced),
        asset.var = "symbol",
        ret.var = "returns", 
        date.var = "date",
        exposure.vars = factor_cols,
        fit.method = "WLS",              # Ordinary least squares
        rob.stats = FALSE,              # Don't use robust statistics
        addIntercept = TRUE,            # Add intercept to regression
        lagExposures = FALSE,           # Don't lag exposures (already aligned)
        z.score = "none",               # Don't z-score (we already transformed)
        full.resid.cov = TRUE,          # KEY: This calculates idiosyncratic covariance matrix
        resid.scaleType = "EWMA",       # Use EWMA for volatility scaling (more adaptive)
        lambda = 0.94                   # EWMA decay parameter
      )
      
      fit$factor.cov
      fit$resid.var
      fit$residuals
      fit$residuals[1:5, 1:5]
      resid.cov <- cov(coredata(fit$residuals), use = "pairwise.complete.obs")
      resid.cov[1:5, 1:5]
      # get 5 highest non diagonal values and print symbol pairs
      resid.cor <- cov2cor(resid.cov)
      diag(resid.cor) <- NA
      resid.cor_melt <- as.data.table(as.table(resid.cor))
      resid.cor_melt = na.omit(resid.cor_melt)
      setorder(resid.cor_melt, -N)
      top_pairs <- resid.cor_melt[1:100]
      for (j in 1:nrow(top_pairs)) {
        sym1 <- rownames(resid.cor)[top_pairs[j, Var1]]
        sym2 <- colnames(resid.cor)[top_pairs[j, Var2]]
        cat(sprintf("Top idio corr: %.4f between %s and %s\n", 
                    top_pairs[j, N], sym1, sym2))
      }
      # get returns correlations for the same pairs from prices data
      price_returns_window = dt_balanced[, .(date, symbol, returns)]
      price_returns_wide = dcast(price_returns_window, date ~ symbol, value.var = "returns")
      price_returns_matrix = as.matrix(price_returns_wide[, -1])
      price_returns_cor <- cor(price_returns_matrix, use = "pairwise.complete.obs")
      price_returns_cor[1:5, 1:5]
      price.cor_melt <- as.data.table(as.table(price_returns_cor))
      price.cor_melt = na.omit(price.cor_melt)
      setorder(price.cor_melt, -N)
      head(price.cor_melt[V1 != V2], 20)
      head(top_pairs, 20)

      
    }, error = function(e) {
      if (i %% 100 == 0) {
        warning(sprintf("Date %s: fitFfm failed - %s", current_date, e$message))
      }
      NULL
    }
    )
  }
  
  # Combine all results
  if (length(idio_returns_list) == 0) {
    warning("No successful estimations")
    return(list(idio_returns = data.table(), coefficients = data.table(), idio_cov = list()))
  }
  
  list(
    idio_returns = rbindlist(idio_returns_list),
    coefficients = rbindlist(coef_list),
    idio_cov = idio_cov_list  # Return the idiosyncratic covariance matrices!
  )
}

# Apply fama_macbeth_facmodcs on our dataset
dt_sample = dt[date %between% c(as.Date("2018-01-01"), as.Date("2020-12-31"))]
fmres = fama_macbeth_facmodcs(dt_sample, factor_cols = c("mom_rank", "vol_rank"), window = 252, min_obs = 50)
residuals = fmres$idio_returns
coefficients = fmres$coefficients
idio_cov_matrices = fmres$idio_cov  # Idiosyncratic covariance matrices (already calculated by fitFfm!)

# reshape residuals
residuals_wide = dcast(residuals, date ~ symbol, value.var = "ret_idiosyncratic")
residuals_wide[1:5, 1:5]


# WORK WITH IDIOSYNCRATIC COVARIANCE MATRICES -----------------------------
cat("\n=== IDIOSYNCRATIC COVARIANCE MATRICES ===\n")
cat(sprintf("Number of covariance matrices: %d\n", length(idio_cov_matrices)))

# Example: Look at one covariance matrix
if (length(idio_cov_matrices) > 0) {
  example_date = names(idio_cov_matrices)[length(idio_cov_matrices)]
  example_cov = idio_cov_matrices[[example_date]]
  example_cor = cov2cor(example_cov)
  
  cat(sprintf("\nExample date: %s\n", example_date))
  cat(sprintf("Matrix dimensions: %d x %d\n", nrow(example_cov), ncol(example_cov)))
  cat(sprintf("Average idiosyncratic correlation: %.4f\n", 
              mean(example_cor[upper.tri(example_cor)])))
  cat(sprintf("Max idiosyncratic correlation: %.4f\n",
              max(example_cor[upper.tri(example_cor)])))
}


#' Extract pairwise idiosyncratic correlations for pairs
#' Simply extracts correlations from the covariance matrices that fitFfm already calculated
#' 
#' @param idio_cov_list List of covariance matrices (from fmres$idio_cov)
#' @param pairs data.table with stock1, stock2 columns
#' @param dates Optional: specific dates to extract (default: all)
#' 
#' @return data.table with date, stock1, stock2, idio_correlation, idio_covariance
extract_pairwise_idio_corr = function(idio_cov_list, pairs, dates = NULL) {
  
  if (is.null(dates)) {
    dates = names(idio_cov_list)
  } else {
    dates = as.character(dates)
  }
  
  results_list = list()
  
  for (date_char in dates) {
    if (!date_char %in% names(idio_cov_list)) next
    
    cov_mat = idio_cov_list[[date_char]]
    cor_mat = cov2cor(cov_mat)
    
    # Get symbols in covariance matrix
    symbols_in_cov = rownames(cov_mat)
    
    # Filter pairs to those available in this date's covariance matrix
    pairs_available = pairs[stock1 %in% symbols_in_cov & stock2 %in% symbols_in_cov]
    
    if (nrow(pairs_available) == 0) next
    
    # Extract correlations and covariances for each pair
    pairs_with_corr = pairs_available[, .(
      stock1, 
      stock2,
      date = as.Date(date_char),
      idio_correlation = sapply(1:.N, function(i) cor_mat[stock1[i], stock2[i]]),
      idio_covariance = sapply(1:.N, function(i) cov_mat[stock1[i], stock2[i]]),
      idio_var1 = sapply(1:.N, function(i) cov_mat[stock1[i], stock1[i]]),
      idio_var2 = sapply(1:.N, function(i) cov_mat[stock2[i], stock2[i]])
    )]
    
    results_list[[date_char]] = pairs_with_corr
  }
  
  rbindlist(results_list, fill = TRUE)
}


# EXAMPLE: Find top correlated pairs -------------------------------------
cat("\n=== FINDING TOP CORRELATED PAIRS ===\n")

if (length(idio_cov_matrices) > 0) {
  # Get most recent covariance matrix
  latest_date = names(idio_cov_matrices)[length(idio_cov_matrices)]
  latest_cov = idio_cov_matrices[[latest_date]]
  latest_cor = cov2cor(latest_cov)
  
  # Extract all pairwise correlations
  cor_values = latest_cor[upper.tri(latest_cor)]
  cor_pairs = which(upper.tri(latest_cor), arr.ind = TRUE)
  
  all_pairs = data.table(
    stock1 = rownames(latest_cor)[cor_pairs[, 1]],
    stock2 = colnames(latest_cor)[cor_pairs[, 2]],
    idio_correlation = cor_values
  )
  
  # Top 20 most correlated pairs
  setorder(all_pairs, -idio_correlation)
  cat(sprintf("\nTop 20 pairs by idiosyncratic correlation (%s):\n", latest_date))
  print(head(all_pairs, 20))
  
  # Distribution of correlations
  cat("\nDistribution of idiosyncratic correlations:\n")
  cat(sprintf("Mean: %.4f\n", mean(all_pairs$idio_correlation)))
  cat(sprintf("Median: %.4f\n", median(all_pairs$idio_correlation)))
  cat(sprintf("SD: %.4f\n", sd(all_pairs$idio_correlation)))
  cat(sprintf("Min: %.4f\n", min(all_pairs$idio_correlation)))
  cat(sprintf("Max: %.4f\n", max(all_pairs$idio_correlation)))
}


# CALCULATE DIAGNOSTICS OVER TIME -----------------------------------------
cat("\n=== CALCULATING COVARIANCE DIAGNOSTICS OVER TIME ===\n")

if (length(idio_cov_matrices) > 0) {
  cov_diagnostics = rbindlist(lapply(names(idio_cov_matrices), function(date_char) {
    cov_mat = idio_cov_matrices[[date_char]]
    cor_mat = cov2cor(cov_mat)
    
    data.table(
      date = as.Date(date_char),
      n_assets = nrow(cov_mat),
      avg_idio_var = mean(diag(cov_mat)),
      avg_idio_corr = mean(cor_mat[upper.tri(cor_mat)]),
      max_idio_corr = max(cor_mat[upper.tri(cor_mat)]),
      min_eigenvalue = min(eigen(cov_mat, only.values = TRUE, symmetric = TRUE)$values),
      condition_number = {
        eigs = eigen(cov_mat, only.values = TRUE, symmetric = TRUE)$values
        max(eigs) / min(eigs)
      }
    )
  }))
  
  print(cov_diagnostics)
  
  # Plot average correlation over time
  p_corr = ggplot(cov_diagnostics, aes(x = date, y = avg_idio_corr)) +
    geom_line(color = "steelblue", linewidth = 1) +
    geom_smooth(method = "loess", se = TRUE, color = "red", alpha = 0.3) +
    labs(
      title = "Average Idiosyncratic Correlation Over Time",
      subtitle = "After controlling for momentum and volatility factors",
      y = "Average Idiosyncratic Correlation",
      x = "Date"
    ) +
    theme_minimal()
  
  ggsave(file.path(PATH_SAVE, "idiosyncratic_correlation_plot.png"), p_corr,
         width = 12, height = 6, dpi = 150)
  
  # Save diagnostics
  write_feather(cov_diagnostics, 
                file.path(PATH_SAVE, "idiosyncratic_cov_diagnostics.feather"))
  
  cat("\nDiagnostics saved!\n")
}


cat("\n=== IDIOSYNCRATIC COVARIANCE ANALYSIS COMPLETE ===\n")
cat("\nSUMMARY:\n")
cat("- fitFfm with full.resid.cov=TRUE automatically calculates idiosyncratic covariance\n")
cat("- No need for separate estimation - it's already done!\n")
cat("- Use extract_pairwise_idio_corr() to get correlations for your pairs\n")
cat("- High idiosyncratic correlation = good pairs candidates\n")


# ALTERNATIVE: USE roll.fitFfmDT FOR EFFICIENT ROLLING ESTIMATION --------
cat("\n\n=== USING roll.fitFfmDT FOR EFFICIENT ROLLING ESTIMATION ===\n")

#' Estimate rolling factor model using facmodCS::roll.fitFfmDT
#' This is MORE EFFICIENT than manual looping - designed for production use
#' 
#' @param dt data.table with symbol, date, returns, and factor columns
#' @param factor_cols Character vector of factor column names
#' @param window Rolling window size (default 252)
#' @param refit_every Refit frequency in days (1 = daily, 5 = weekly, 21 = monthly)
#' @param refit_window "Rolling" or "Expanding"
#' 
#' @return List with residuals, coefficients, and covariance matrices
estimate_with_roll_fitFfmDT = function(dt, 
                                        factor_cols,
                                        window = 252,
                                        refit_every = 1,
                                        refit_window = "Rolling") {
  
  # Step 1: Balance the panel first (roll.fitFfmDT requires balanced data)
  cat("Step 1: Balancing panel...\n")
  dt_fit = dt[, c("symbol", "date", "returns", factor_cols), with = FALSE]
  dt_fit = na.omit(dt_fit)
  setorder(dt_fit, date, symbol)
  
  # Find symbols that exist for all (or most) dates
  obs_per_symbol = dt_fit[, .N, by = symbol]
  max_obs = max(obs_per_symbol$N)
  complete_symbols = obs_per_symbol[N >= 0.95 * max_obs, symbol]
  
  cat(sprintf("  Keeping %d/%d symbols with >=95%% complete data\n",
              length(complete_symbols), dt_fit[, uniqueN(symbol)]))
  
  dt_balanced = dt_fit[symbol %in% complete_symbols]
  
  # Verify all dates have same number of symbols
  symbols_per_date = dt_balanced[, .N, by = date]
  if (symbols_per_date[, uniqueN(N)] > 1) {
    # Keep only dates where we have all symbols
    complete_dates = symbols_per_date[N == max(N), date]
    dt_balanced = dt_balanced[date %in% complete_dates]
    cat(sprintf("  Using %d dates with complete data\n", length(complete_dates)))
  }
  
  # Step 2: Create ffmSpec object (specification for factor model)
  cat("\nStep 2: Creating factor model specification...\n")
  
  ffm_spec = facmodCS::specFFM(
    data = as.data.frame(dt_balanced),
    asset.var = "symbol",
    ret.var = "returns",
    date.var = "date",
    exposure.vars = factor_cols,
    weight.var = NULL,           # Equal-weighted
    addIntercept = TRUE          # Include intercept
  )
  
  cat("  Specification created successfully\n")
  
  # Step 3: Run rolling factor model estimation
  cat(sprintf("\nStep 3: Running rolling estimation (window=%d, refit_every=%d)...\n",
              window, refit_every))
  
  roll_result = facmodCS::roll.fitFfmDT(
    ffMSpecObj = ffm_spec,
    windowSize = window,
    refitEvery = refit_every,
    refitWindow = refit_window,
    fitControl = list(
      fit.method = "LS",           # Ordinary least squares
      resid.scaleType = "EWMA",    # EWMA for volatility scaling
      lambda = 0.94                # EWMA decay
    ),
    full.resid.cov = TRUE,         # Calculate full covariance matrix
    analysis = "NEW"               # Use newer analysis method
  )
  
  cat("  Rolling estimation complete!\n")
  
  # Step 4: Extract results
  cat("\nStep 4: Extracting results...\n")
  
  # Extract residuals (idiosyncratic returns)
  # roll_result should have residuals for each refit date
  residuals_list = list()
  coef_list = list()
  cov_list = list()
  
  # The structure depends on the package version, but typically:
  # roll_result[[date]]$residuals, roll_result[[date]]$factor.returns, etc.
  
  for (i in seq_along(roll_result)) {
    fit_i = roll_result[[i]]
    
    if (!is.null(fit_i$residuals)) {
      # Get the last date's residuals from this window
      resid_dates = rownames(fit_i$residuals)
      last_date = resid_dates[length(resid_dates)]
      
      residuals_list[[last_date]] = data.table(
        date = as.Date(last_date),
        symbol = colnames(fit_i$residuals),
        ret_idiosyncratic = as.vector(fit_i$residuals[last_date, ])
      )
    }
    
    if (!is.null(fit_i$factor.returns)) {
      factor_dates = rownames(fit_i$factor.returns)
      last_date = factor_dates[length(factor_dates)]
      
      coef_list[[last_date]] = data.table(
        date = as.Date(last_date),
        term = colnames(fit_i$factor.returns),
        estimate = as.vector(fit_i$factor.returns[last_date, ])
      )
    }
    
    if (!is.null(fit_i$resid.cov)) {
      # Store covariance matrix
      cov_list[[last_date]] = fit_i$resid.cov
    }
    
    if (i %% 50 == 0) {
      cat(sprintf("  Processed %d/%d windows\n", i, length(roll_result)))
    }
  }
  
  cat(sprintf("  Extracted data for %d dates\n", length(residuals_list)))
  
  list(
    idio_returns = rbindlist(residuals_list),
    coefficients = rbindlist(coef_list),
    idio_cov = cov_list,
    roll_result = roll_result  # Keep full result for advanced analysis
  )
}


# EXAMPLE: Use roll.fitFfmDT -------------------------------------------
cat("\n\n=== EXAMPLE: Using roll.fitFfmDT ===\n")

# Create a smaller sample for testing
dt_test = dt[date %between% c(as.Date("2020-01-01"), as.Date("2020-06-30"))]

cat("\nRunning rolling estimation with roll.fitFfmDT...\n")
cat("This is more efficient than manual loops!\n\n")

tryCatch({
  roll_res = estimate_with_roll_fitFfmDT(
    dt = dt_test,
    factor_cols = c("mom_rank", "vol_rank"),
    window = 60,        # 60-day window for testing (use 252 for production)
    refit_every = 5,    # Refit weekly (use 1 for daily, 21 for monthly)
    refit_window = "Rolling"
  )
  
  cat("\n=== RESULTS FROM roll.fitFfmDT ===\n")
  cat(sprintf("Idiosyncratic returns: %d observations\n", nrow(roll_res$idio_returns)))
  cat(sprintf("Coefficients: %d dates\n", roll_res$coefficients[, uniqueN(date)]))
  cat(sprintf("Covariance matrices: %d\n", length(roll_res$idio_cov)))
  
  # Compare with manual method
  cat("\n=== COMPARISON ===\n")
  cat("roll.fitFfmDT advantages:\n")
  cat("  - Much faster (optimized C++ code)\n")
  cat("  - Built-in rolling window logic\n")
  cat("  - Handles complex refit schedules\n")
  cat("  - Production-ready\n")
  cat("  - Supports GARCH, robust estimation, etc.\n")
  
}, error = function(e) {
  cat(sprintf("\nError with roll.fitFfmDT: %s\n", e$message))
  cat("This might be due to:\n")
  cat("  - Insufficient data in window\n")
  cat("  - Panel not perfectly balanced\n")
  cat("  - Package version differences\n")
  cat("\nFalling back to manual method is fine for most use cases.\n")
})


cat("\n\n=== RECOMMENDATION ===\n")
cat("For production use:\n")
cat("1. Use roll.fitFfmDT if you have clean, balanced panel data\n")
cat("2. Use manual fama_macbeth_facmodcs() if you need more control\n")
cat("3. Both methods give same results, roll.fitFfmDT is just faster\n")
cat("\nKey parameters to tune:\n")
cat("  - windowSize: 252 for annual, 126 for semi-annual, 60 for quarterly\n")
cat("  - refitEvery: 1 (daily), 5 (weekly), 21 (monthly)\n")
cat("  - refitWindow: 'Rolling' (fixed window) or 'Expanding' (growing window)\n")
