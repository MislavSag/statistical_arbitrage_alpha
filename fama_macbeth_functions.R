# Fama-MacBeth Cross-Sectional Regression Functions
# For extracting idiosyncratic returns and calculating factor premiums

#' Calculate cross-sectional residuals (idiosyncratic returns) using Fama-MacBeth
#' 
#' @param dt data.table with columns: date, symbol, returns, and factor variables
#' @param formula Formula for cross-sectional regression (e.g., "returns ~ spy_returns + mom_rank + vol_rank")
#' @param window Number of periods to use for rolling estimation (NULL = expanding window)
#' @param min_obs Minimum number of observations required for regression
#' @param extract_residuals Logical, return residuals (idiosyncratic returns)?
#' @param extract_fitted Logical, return fitted values (expected returns)?
#' @param extract_coefficients Logical, return factor premiums over time?
#' 
#' @return data.table with original data plus requested outputs
fama_macbeth_cs = function(dt, 
                           formula,
                           window = NULL,
                           min_obs = 50,
                           extract_residuals = TRUE,
                           extract_fitted = FALSE,
                           extract_coefficients = FALSE) {
  
  # Sort by date
  setorder(dt, date, symbol)
  
  # Get unique dates
  dates = sort(unique(dt$date))
  
  # Initialize output columns
  if (extract_residuals) dt[, ret_idiosyncratic := NA_real_]
  if (extract_fitted) dt[, ret_expected := NA_real_]
  
  # Store coefficients if requested
  if (extract_coefficients) {
    coef_list = list()
  }
  
  # Loop through each date for cross-sectional regression
  for (j in seq_along(dates)) {
    current_date = dates[j]
    
    # Determine training window
    if (is.null(window)) {
      # Expanding window: use all data up to current date
      train_dates = dates[1:j]
    } else {
      # Rolling window: use last 'window' periods
      start_idx = max(1, j - window + 1)
      train_dates = dates[start_idx:j]
    }
    
    # Get training data
    train_data = dt[date %in% train_dates]
    
    # Get current date data for prediction
    current_data = dt[date == current_date]
    
    if (nrow(current_data) < min_obs) next
    
    # Fit cross-sectional model at current date
    tryCatch({
      # Use only current date for true Fama-MacBeth (cross-sectional)
      model = lm(as.formula(formula), data = current_data)
      
      # Extract residuals (idiosyncratic returns)
      if (extract_residuals) {
        residuals_vec = residuals(model)
        dt[date == current_date, ret_idiosyncratic := residuals_vec]
      }
      
      # Extract fitted values (expected returns)
      if (extract_fitted) {
        fitted_vec = fitted(model)
        dt[date == current_date, ret_expected := fitted_vec]
      }
      
      # Store coefficients (factor premiums)
      if (extract_coefficients) {
        coef_list[[as.character(current_date)]] = data.table(
          date = current_date,
          term = names(coef(model)),
          estimate = coef(model),
          r_squared = summary(model)$r.squared,
          n_obs = nrow(current_data)
        )
      }
      
    }, error = function(e) {
      # Skip if regression fails
      NULL
    })
  }
  
  # Return results
  if (extract_coefficients) {
    list(
      data = dt,
      coefficients = rbindlist(coef_list)
    )
  } else {
    dt
  }
}


#' Calculate rolling idiosyncratic covariance matrix
#' 
#' @param dt data.table with idiosyncratic returns (from fama_macbeth_cs)
#' @param symbols Vector of symbols to include in covariance matrix
#' @param window Number of periods for rolling covariance
#' @param min_obs Minimum number of observations required
#' 
#' @return List of covariance matrices by date
rolling_idio_cov = function(dt, 
                            symbols = NULL,
                            window = 60,
                            min_obs = 30) {
  
  if (is.null(symbols)) {
    symbols = dt[, unique(symbol)]
  }
  
  # Filter to requested symbols
  dt_filtered = dt[symbol %in% symbols]
  
  # Pivot to wide format
  dt_wide = dcast(dt_filtered, date ~ symbol, value.var = "ret_idiosyncratic")
  
  # Calculate rolling covariance
  dates = dt_wide$date
  cov_list = list()
  
  for (i in seq_along(dates)) {
    if (i < window) next
    
    # Get window of returns
    start_idx = i - window + 1
    returns_matrix = as.matrix(dt_wide[start_idx:i, -1])  # Exclude date column
    
    # Remove columns with too many NAs
    na_count = colSums(is.na(returns_matrix))
    valid_cols = na_count < (window - min_obs)
    
    if (sum(valid_cols) < 2) next
    
    returns_matrix = returns_matrix[, valid_cols, drop = FALSE]
    
    # Calculate covariance (pairwise complete obs)
    cov_mat = cov(returns_matrix, use = "pairwise.complete.obs")
    
    cov_list[[as.character(dates[i])]] = list(
      date = dates[i],
      symbols = colnames(returns_matrix),
      cov_matrix = cov_mat
    )
  }
  
  cov_list
}


#' Calculate idiosyncratic correlation for pairs
#' 
#' @param dt data.table with idiosyncratic returns
#' @param pairs data.table with columns stock1, stock2
#' @param window Number of periods for rolling correlation
#' @param min_obs Minimum number of observations required
#' 
#' @return data.table with pair correlations over time
rolling_idio_pair_cor = function(dt,
                                 pairs,
                                 window = 60,
                                 min_obs = 30) {
  
  results = list()
  
  for (i in 1:nrow(pairs)) {
    stock1 = pairs[i, stock1]
    stock2 = pairs[i, stock2]
    
    # Get returns for both stocks
    dt1 = dt[symbol == stock1, .(date, ret1 = ret_idiosyncratic)]
    dt2 = dt[symbol == stock2, .(date, ret2 = ret_idiosyncratic)]
    
    # Merge
    dt_pair = merge(dt1, dt2, by = "date")
    setorder(dt_pair, date)
    
    # Calculate rolling correlation
    dt_pair[, correlation := {
      n = .N
      if (n < window) {
        rep(NA_real_, n)
      } else {
        sapply(window:n, function(j) {
          if (sum(!is.na(ret1[(j-window+1):j]) & !is.na(ret2[(j-window+1):j])) >= min_obs) {
            cor(ret1[(j-window+1):j], ret2[(j-window+1):j], use = "complete.obs")
          } else {
            NA_real_
          }
        })
      }
    }]
    
    results[[i]] = dt_pair[, .(
      stock1 = stock1,
      stock2 = stock2,
      date,
      idio_correlation = correlation
    )]
  }
  
  rbindlist(results)
}


#' Summary statistics for Fama-MacBeth coefficients
#' 
#' @param coef_dt data.table of coefficients from fama_macbeth_cs
#' 
#' @return data.table with mean, sd, t-stat for each factor
summarize_fama_macbeth = function(coef_dt) {
  
  coef_dt[term != "(Intercept)", {
    n = .N
    mean_est = mean(estimate, na.rm = TRUE)
    sd_est = sd(estimate, na.rm = TRUE)
    
    .(
      mean_premium = mean_est,
      sd_premium = sd_est,
      t_stat = mean_est / (sd_est / sqrt(n)),
      n_periods = n,
      mean_r_squared = mean(r_squared, na.rm = TRUE)
    )
  }, by = term]
}
