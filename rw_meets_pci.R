library(fastverse)
library(finutils)
library(xts)
library(partialCI)


# DATA --------------------------------------------------------------------
# RW pairs
PATH_SAVE = "D:/strategies/statsarb"
pairs_features = fread(file.path(PATH_SAVE, "pairs_features.csv"))


# Prices
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 252,
  duplicates = "fast",
  price_threshold = 1e-008
)
prices = prices[date > as.Date("2010-01-01")]
prices[, fmp_symbol := gsub("\\.\\d$", "", symbol)]
prices[, fmp_symbol := toupper(fmp_symbol)]


# FILTERING ---------------------------------------------------------------
# Best pairs
best = pairs_features[same_industry == 1 &
                        isfund1 == FALSE & isfund2 == FALSE &
                        name1 != name2 & isin1 != isin2,
                      .(stock1, stock2, combo_score_2021, combo_score_2022,
                        combo_score_2023, combo_score_2024)]
best = melt(best, id.vars = c("stock1", "stock2"))
best = na.omit(best)
best[, year := as.integer(gsub("combo_score_", "", variable))]
best[, variable := NULL]
setorder(best, year, -value)

# Extract 1000 best
best = best[, head(.SD, 1000), by = year]



# ESTIMATE PCI ------------------------------------------------------------
# Estimate pci for best paris
setkey(prices, fmp_symbol)
symbols = prices[, unique(fmp_symbol)]
pci_results = list()
for (i in 1:nrow(best)) {
  print(i)

  # Extract pair
  pair_ = best[i]

  # test for symbols
  if (!(pair_[, stock1] %in% symbols &  pair_[, stock2] %in% symbols)) {
    print("Symbols not in data")
    pci_results[[i]] = data.table(
      stock1 = pair_[, stock1],
      stock2 = pair_[, stock2]
    )
    next
  }

  # Prepare data for PCI
  dt_ = prices[pair_[, c(stock1, stock2)], .(date, fmp_symbol, close)]
  year_ = pair_[, year]
  start_date = as.Date(paste0(year_-1, "-01-01")) - 30
  end_date   = as.Date(paste0(year_-1, "-12-31"))
  dt_ = dt_[date %between% c(start_date, end_date)]
  if (anyDuplicated(dt_, by = c("date", "fmp_symbol"))) {
    print("Duplicated data")
    pci_results[[i]] = data.table(
      stock1 = pair_[, stock1],
      stock2 = pair_[, stock2]
    )
    next
  }
  dt_ = dcast(dt_, date ~ fmp_symbol, value.var = "close")
  dt_ = na.omit(dt_)
  if (nrow(dt_) < 200) {
    print("Too few data")
    pci_results[[i]] = data.table(
      stock1 = pair_[, stock1],
      stock2 = pair_[, stock2]
    )
    next
  }
  train = as.xts.data.table(dt_)
  train = log(train)

  # Test pci
  fit_pci  = fit.pci(train[, 1], train[, 2])
  test_pci = test.pci(train[, 1], train[, 2])
  p_rw = test_pci$p.value[1]
  p_ar = test_pci$p.value[2]
  p_joint = test_pci$p.value[3]

  # summary table
  results = data.table(
    stock1 = fit_pci$target_name,
    stock2 = fit_pci$factor_names
  )

  # summary table
  metrics = c(
    fit_pci$beta,
    fit_pci$rho,
    fit_pci$sigma_M,
    fit_pci$sigma_R,
    fit_pci$M0,
    fit_pci$R0,
    fit_pci$beta.se,
    fit_pci$rho.se,
    fit_pci$sigma_M.se,
    fit_pci$sigma_R.se,
    fit_pci$M0.se,
    fit_pci$R0.se,
    fit_pci$negloglik,
    fit_pci$pvmr
  )

  # Change names
  names(metrics)[names(metrics) %in% names(fit_pci$beta)] = paste0("beta_", seq_along(fit_pci$beta))
  names(metrics)[names(metrics) %in% names(fit_pci$beta.se)] = paste0("beta_", seq_along(fit_pci$beta.se), "_se")
  names(metrics)[names(metrics) %in% names(fit_pci$rho.se)][2] = "rho_se"
  names(metrics)[names(metrics) %in% names(fit_pci$sigma_M.se)][2] = "sigma_M_se"
  names(metrics)[names(metrics) %in% names(fit_pci$sigma_R.se)][2] = "sigma_R_se"
  names(metrics)[names(metrics) %in% names(fit_pci$M0.se)][2] = "M0_se"
  names(metrics)[names(metrics) %in% names(fit_pci$R0.se)][2] = "R0_se"

  # Convert to data.table, add resulsts to metricsa and p values
  metrics = as.data.table(as.list(metrics))
  pci_results[[i]] = cbind(results,
                           year = year_,
                           metrics,
                           p_rw = test_pci$p.value[1],
                           p_ar = test_pci$p.value[2])
}
pci_results_dt = rbindlist(pci_results, fill = TRUE)

# Remove missing
pci_results_dt = na.omit(pci_results_dt, cols = c("beta_1"))


# FILTER PAIRS ------------------------------------------------------------
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
pci_tests_eligible = pci_results_dt[pvmr > 0.5  & rho > 0.5 &  p_rw < 0.05 & p_ar < 0.05]
pci_tests_eligible[, nq := quantile(negloglik, probs = 0.25)]
pci_tests_eligible = pci_tests_eligible[negloglik <= nq]

# remove same pairs
pci_tests_eligible = pci_tests_eligible[, .SD[!(stock2 %in% stock1)]]

# Merge best and pci filteresd
best_pci = merge(best, pci_tests_eligible,
                 by.x = c("stock1", "stock2", "year"),
                 by.y = c("stock1", "stock2", "year"),
                 all.x = TRUE,
                 all.y = FALSE
)
best_pci = na.omit(best_pci, cols = c("beta_1"))

# Save for azure
best_pci[year == 2024]
fwrite(best_pci, file.path(PATH_SAVE, "pairs_best_pci.csv"))
