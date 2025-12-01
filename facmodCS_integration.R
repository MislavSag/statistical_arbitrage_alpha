# Using facmodCS Package for Factor Model Estimation
# Documentation: https://cran.r-project.org/package=facmodCS

library(facmodCS)
library(data.table)

#' Estimate factor model and extract idiosyncratic covariance using facmodCS
#' 
#' @param dt data.table with columns: date, symbol, returns, and factor variables
#' @param factors Character vector of factor column names (e.g., c("spy_returns", "mom_rank", "vol_rank"))
#' @param window Number of periods for rolling estimation (NULL = single period)
#' @param method Estimation method: "OLS", "Ridge", "Lasso", "ElasticNet"
#' @param lambda Regularization parameter (for Ridge/Lasso/ElasticNet)
#' 
#' @return List with idiosyncratic returns and covariance matrices
facmod_estimate = function(dt,
                          factors,
                          window = NULL,
                          method = "OLS",
                          lambda = NULL) {
  
  setorder(dt, date, symbol)
  dates = sort(unique(dt$date))
  
  # Store results
  idio_returns_list = list()
  factor_loadings_list = list()
  idio_cov_list = list()
  
  for (i in seq_along(dates)) {
    current_date = dates[i]
    
    # Get data for this date
    dt_date = dt[date == current_date]
    
    if (nrow(dt_date) < 30) next
    
    # Prepare data for facmodCS
    # Returns (T x N matrix - here T=1 for cross-sectional)
    returns_matrix = matrix(dt_date$returns, nrow = 1)
    colnames(returns_matrix) = dt_date$symbol
    
    # Factors (T x K matrix)
    factor_matrix = as.matrix(dt_date[, ..factors])
    
    # Check for complete cases
    complete_idx = complete.cases(returns_matrix) & complete.cases(factor_matrix)
    if (sum(complete_idx) < 30) next
    
    # Estimate factor model
    tryCatch({
      if (method == "OLS") {
        # Standard OLS cross-sectional regression
        fit = facmodCS(
          returns = returns_matrix,
          factors = factor_matrix,
          method = "OLS"
        )
      } else {
        # Regularized methods
        fit = facmodCS(
          returns = returns_matrix,
          factors = factor_matrix,
          method = method,
          lambda = lambda
        )
      }
      
      # Extract idiosyncratic returns (residuals)
      idio_ret = fit$residuals
      
      # Store results
      idio_returns_list[[as.character(current_date)]] = data.table(
        date = current_date,
        symbol = dt_date$symbol,
        ret_idiosyncratic = as.vector(idio_ret)
      )
      
      # Store factor loadings (betas)
      factor_loadings_list[[as.character(current_date)]] = data.table(
        date = current_date,
        symbol = dt_date$symbol,
        as.data.table(fit$loadings)
      )
      
    }, error = function(e) {
      NULL
    })
  }
  
  # Combine results
  idio_returns = rbindlist(idio_returns_list)
  factor_loadings = rbindlist(factor_loadings_list)
  
  # Calculate rolling idiosyncratic covariance if window specified
  if (!is.null(window)) {
    idio_cov_list = calculate_rolling_idio_cov(idio_returns, window)
  }
  
  list(
    idio_returns = idio_returns,
    factor_loadings = factor_loadings,
    idio_covariance = idio_cov_list
  )
}


#' Calculate rolling idiosyncratic covariance matrix
#' 
#' @param idio_returns data.table from facmod_estimate
#' @param window Number of periods for rolling window
#' 
#' @return List of covariance matrices by date
calculate_rolling_idio_cov = function(idio_returns, window = 60) {
  
  # Pivot to wide format
  idio_wide = dcast(idio_returns, date ~ symbol, value.var = "ret_idiosyncratic")
  
  dates = idio_wide$date
  cov_list = list()
  
  for (i in seq_along(dates)) {
    if (i < window) next
    
    # Get window of returns
    start_idx = i - window + 1
    returns_mat = as.matrix(idio_wide[start_idx:i, -1])
    
    # Remove columns with too many NAs
    na_pct = colMeans(is.na(returns_mat))
    valid_cols = na_pct < 0.5
    
    if (sum(valid_cols) < 2) next
    
    returns_mat = returns_mat[, valid_cols, drop = FALSE]
    
    # Calculate covariance
    cov_mat = cov(returns_mat, use = "pairwise.complete.obs")
    
    cov_list[[as.character(dates[i])]] = list(
      date = dates[i],
      symbols = colnames(returns_mat),
      cov_matrix = cov_mat,
      cor_matrix = cov2cor(cov_mat)  # Also store correlation
    )
  }
  
  cov_list
}


#' Alternative: Use facmodCS for time-series factor model (multiple periods)
#' 
#' @param dt data.table with date, symbol, returns, and factors
#' @param factors Character vector of factor names
#' @param window Number of time periods to use
#' 
#' @return facmodCS model object
facmod_timeseries = function(dt, factors, window = 252) {
  
  # Prepare data: T x N returns matrix
  returns_wide = dcast(dt, date ~ symbol, value.var = "returns")
  dates = returns_wide$date
  returns_matrix = as.matrix(returns_wide[, -1])
  rownames(returns_matrix) = as.character(dates)
  
  # Prepare factors: T x K matrix
  # For time-series, factors should vary over time
  # If spy_returns is constant across stocks, take first value per date
  factor_dt = dt[, lapply(.SD, function(x) x[1]), .SDcols = factors, by = date]
  setorder(factor_dt, date)
  factor_matrix = as.matrix(factor_dt[, -1])
  rownames(factor_matrix) = as.character(factor_dt$date)
  
  # Use only last 'window' periods if specified
  if (!is.null(window) && nrow(returns_matrix) > window) {
    returns_matrix = returns_matrix[(nrow(returns_matrix)-window+1):nrow(returns_matrix), ]
    factor_matrix = factor_matrix[(nrow(factor_matrix)-window+1):nrow(factor_matrix), ]
  }
  
  # Estimate factor model
  fit = facmodCS(
    returns = returns_matrix,
    factors = factor_matrix,
    method = "OLS"
  )
  
  fit
}


#' Estimate idiosyncratic covariance with shrinkage (Ledoit-Wolf style)
#' 
#' @param idio_returns data.table of idiosyncratic returns
#' @param shrinkage Shrinkage intensity (0 = sample cov, 1 = diagonal)
#' 
#' @return Shrunk covariance matrix
shrink_idio_cov = function(idio_returns, shrinkage = NULL) {
  
  # Pivot to wide
  idio_wide = dcast(idio_returns, date ~ symbol, value.var = "ret_idiosyncratic")
  returns_mat = as.matrix(idio_wide[, -1])
  
  # Sample covariance
  S = cov(returns_mat, use = "pairwise.complete.obs")
  
  # If shrinkage not specified, use automatic selection (Ledoit-Wolf)
  if (is.null(shrinkage)) {
    # Simple diagonal target
    target = diag(diag(S))
    
    # Ledoit-Wolf shrinkage intensity
    n = nrow(returns_mat)
    p = ncol(returns_mat)
    
    # Simplified shrinkage (proper LW is more complex)
    shrinkage = min(1, max(0, (p/n)))
  }
  
  # Shrink toward diagonal
  target = diag(diag(S))
  S_shrunk = (1 - shrinkage) * S + shrinkage * target
  
  list(
    cov = S_shrunk,
    cor = cov2cor(S_shrunk),
    shrinkage = shrinkage
  )
}


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if (FALSE) {
  
  # Example 1: Cross-sectional factor model with facmodCS
  # ------------------------------------------------------
  
  # Prepare your data
  factors = c("spy_returns", "mom_rank", "vol_rank")
  
  results = facmod_estimate(
    dt = prices,
    factors = factors,
    window = 60,  # 60-day rolling covariance
    method = "OLS"
  )
  
  # Extract components
  idio_returns = results$idio_returns
  factor_loadings = results$factor_loadings
  idio_cov_matrices = results$idio_covariance
  
  # Get latest covariance matrix
  latest_cov = idio_cov_matrices[[length(idio_cov_matrices)]]
  
  
  # Example 2: Ridge regression (for high-dimensional case)
  # --------------------------------------------------------
  
  results_ridge = facmod_estimate(
    dt = prices,
    factors = c("spy_returns", "mom_rank", "vol_rank", sector_cols),  # Many factors
    method = "Ridge",
    lambda = 0.1
  )
  
  
  # Example 3: Time-series factor model
  # ------------------------------------
  
  # Estimate using multiple time periods
  fit_ts = facmod_timeseries(
    dt = prices,
    factors = c("spy_returns", "mom_rank", "vol_rank"),
    window = 252
  )
  
  # Extract idiosyncratic covariance
  idio_cov_ts = fit_ts$Sigma_eps
  
  
  # Example 4: Shrinkage for better estimation
  # -------------------------------------------
  
  idio_cov_shrunk = shrink_idio_cov(
    idio_returns = results$idio_returns,
    shrinkage = NULL  # Automatic
  )
  
  # Use shrunk covariance for portfolio optimization
  Sigma = idio_cov_shrunk$cov
  
}
