library(fastverse)
library(finutils)
library(partialCI)
library(xts)
library(AzureStor)
library(lubridate)
library(ggplot2)
library(patchwork)
library(PerformanceAnalytics)


# PRICE DATA --------------------------------------------------------------
# Import dailz data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 2 * 252,
  price_threshold = 1e-008,
  add_dv_rank = TRUE
)


# PREPARE DATA ------------------------------------------------------------
# Keep only last 2 years of data
prices = prices[date >= (Sys.Date() - (2 * 365))]

# Inspect
prices[symbol == "aapl"]
prices[, max(date)]

# To long format
prices = dcast(prices[, .(symbol, date, close)], date ~ symbol, value.var = "close")

# Keep only symbols with almost no NA values
keep_cols = names(which(colMeans(!is.na(prices)) > 0.99))
prices = prices[, .SD, .SDcols = keep_cols]

# Convert to xts and take logs
train = as.xts.data.table(prices) # convert to xts
train = train[, colSums(!is.na(train)) == max(colSums(!is.na(train)))]
train = log(train)


dim(train)

# COARSE BEST PAIRS -------------------------------------------------------
# choose best pairs using hedge.pci function and maxfact = 1 (only one factor possible)
pci_tests_i = list()
s = Sys.time()
for (j in 1:ncol(train)) { #

  # DEBUG
  print(j)
  # j = which(names(train) == "abh")

  # quasi multivariate pairs
  hedge = tryCatch(hedge.pci(train[, j], train[, -j],
                             maxfact = 1,
                             use.multicore = FALSE,
                             search_type = "lasso"),
                   error = function(e) NULL)
  if (is.null(hedge)) {
    pci_tests_i[[j]] = NULL
    next()
  }

  # pci fit
  test_pci = test.pci(train[, j], hedge$pci$basis)

  # summary table
  results = data.table(t(hedge$index_names))
  names(results) = paste0("series_", seq_along(results) + 1)
  results = cbind(series_1 = hedge$pci$target_name, results)

  # summary table
  metrics = c(
    hedge$pci$beta,
    hedge$pci$rho,
    hedge$pci$sigma_M,
    hedge$pci$sigma_R,
    hedge$pci$M0,
    hedge$pci$R0,
    hedge$pci$beta.se,
    hedge$pci$rho.se,
    hedge$pci$sigma_M.se,
    hedge$pci$sigma_R.se,
    hedge$pci$M0.se,
    hedge$pci$R0.se,
    hedge$pci$negloglik,
    hedge$pci$pvmr
  )

  # Change names
  names(metrics)[names(metrics) %in% names(hedge$pci$beta)] = paste0("beta_", seq_along(hedge$pci$beta))
  names(metrics)[names(metrics) %in% names(hedge$pci$beta.se)] = paste0("beta_", seq_along(hedge$pci$beta.se), "_se")
  names(metrics)[names(metrics) %in% names(hedge$pci$rho.se)][2] = "rho_se"
  names(metrics)[names(metrics) %in% names(hedge$pci$sigma_M.se)][2] = "sigma_M_se"
  names(metrics)[names(metrics) %in% names(hedge$pci$sigma_R.se)][2] = "sigma_R_se"
  names(metrics)[names(metrics) %in% names(hedge$pci$M0.se)][2] = "M0_se"
  names(metrics)[names(metrics) %in% names(hedge$pci$R0.se)][2] = "R0_se"

  # Convert to data.table, add resulsts to metricsa and p values
  metrics = as.data.table(as.list(metrics))
  results = cbind(results, metrics)

  # Save results
  pci_tests_i[[j]] = cbind(results,
                           p_rw = test_pci$p.value[1],
                           p_ar = test_pci$p.value[2])
}
e = Sys.time()
print(e - s)
pci_tests = rbindlist(pci_tests_i, fill = TRUE)

# Save
file_name = paste0("pci-", Sys.Date(), ".csv")
file_name = file.path("D:/strategies/pci", file_name)
fwrite(pci_tests, file_name)


# FILTER PAIRS ------------------------------------------------------------
# https://www.econstor.eu/bitstream/10419/140632/1/858609614.pdf
# page 14 above
# Apply restrictions to the universe
# 1) pairs with a half-life of mean-reversion of one day or less - thereby avoiding to select
#where trading gains are largely attributable to bid-ask bounce
# 2) pvmr > 0.5 ensures    pairs  more reliable parameter estimates
# 3) p_rw < 0.05 & p_ar < 0.05. A time series is classified as partially cointegrated,
#    if and only if the random walk as well as the AR(1)-hypotheses are rejected
# 3) my condition: MR p value should be lower than 0.05 because this test confirms mean reverting component
# 4) restriction to same sector I DON'T WANT APPLY THIS FOR  NOW
# 5) 25% lowest  by neLog
# 6) possible to add additional fundamental matching
pci_tests_eligible = pci_tests[pvmr > 0.5  & rho > 0.5 & p_rw < 0.05 & p_ar < 0.05]
pci_tests_eligible[, nq := quantile(negloglik, probs = 0.25)]
pci_tests_eligible = pci_tests_eligible[negloglik <= nq]

# remove same pairs
pci_tests_eligible = pci_tests_eligible[, .SD[!(series_2 %in% series_1)]]



# INSAMPLE SHARPE ---------------------------------------------------------
# Function to generate zscore
generate_signal_zscore = function(Z_score, threshold_long, threshold_short) {
  signal = Z_score
  colnames(signal) = "signal"
  signal[] = NA

  # initial position
  signal[1] = 0
  if (Z_score[1] <= threshold_long[1]) {
    signal[1] = 1
  } else if (Z_score[1] >= threshold_short[1])
    signal[1] = -1

  # loop
  for (t in 2:nrow(Z_score)) {
    if (signal[t-1] == 0) {  # if we were in no position
      if (Z_score[t] <= threshold_long[t]) {
        signal[t] = 1
      } else if(Z_score[t] >= threshold_short[t]) {
        signal[t] = -1
      } else signal[t] = 0
    } else if (signal[t-1] == 1) {  #if we were in a long position
      if (Z_score[t] >= 0) signal[t] = 0
      else signal[t] = signal[t-1]
    } else {  #if we were in a short position
      if (Z_score[t] <= 0) signal[t] = 0
      else signal[t] = signal[t-1]
    }
  }
  return(signal)
}

# main function to analyse pairs
pairs_trading_pci = function(n, std_entry = 2, plot_pnl = TRUE) {
  # Get tickers
  ticker_1 = pci_tests_eligible[n, series_1]
  ticker_2 = pci_tests_eligible[n, series_2]

  # Fit pci and get spreads
  fit_pci = fit.pci(train[, ticker_1], train[, ticker_2])
  hs_train = statehistory.pci(fit_pci)
  spread = xts(hs_train[, 4], as.Date(rownames(hs_train)))

  # Z-score
  spread_var = sd(spread)
  Z_score_M = spread/spread_var

  # generate signals with z scored
  threshold_long = threshold_short = Z_score_M
  threshold_short[] = std_entry
  threshold_long[] = -std_entry

  # get and plot signals
  signal = generate_signal_zscore(Z_score_M, threshold_long, threshold_short)

  # let's compute the PnL directly from the signal and spread
  spread_return = diff(Z_score_M)
  traded_return = spread_return * lag(signal)   # NOTE THE LAG!!
  traded_return[is.na(traded_return)] = 0
  colnames(traded_return) = "traded spread"

  # Total
  res_ = mean(traded_return, na.rm = TRUE)
  sd_ = sd(traded_return, na.rm = TRUE)

  return(res_ / sd_)
}

# Get insample results
results = vapply(1:nrow(pci_tests_eligible),
                 function(i) pairs_trading_pci(i),
                 FUN.VALUE = numeric(1L))
results_dt = cbind(pci_tests_eligible, insample_performance = results)

# Inspect 20 best
setorder(results_dt, -insample_performance, na.last = TRUE)
results_dt


# SAVE --------------------------------------------------------------------
# Save locally and to Azure blob
bl_endp_key = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"),
                               Sys.getenv("BLOB-KEY"))
cont = storage_container(bl_endp_key, "qc-live")
file_name_ = paste0("pci-", update(Sys.Date(), day = 2), ".csv")
storage_write_csv(as.data.frame(head(results_dt, 20)), cont, file_name_)
