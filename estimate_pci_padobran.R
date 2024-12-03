suppressMessages(library(data.table))
suppressMessages(library(xts))
suppressMessages(library(partialCI))


# IMPORT DATA -------------------------------------------------------------
# Import prices and meta
if (interactive()) {
  prices = fread("F:/strategies/statsarb/pci/prices.csv")
  meta = fread("F:/strategies/statsarb/pci/meta.csv")
} else {
  meta = fread("meta.csv")
  prices = fread("prices.csv")
}


# PREPARE DATA ------------------------------------------------------------
# i'th row from meta
if (interactive()) {
  i = 1234
} else {
  i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
}

# Take data_sample
train_dt = prices[date %between% c(meta[i, train_start], meta[i, train_end])]
train_dt = dcast(train_dt[, .(symbol, date, close)],
                 date ~ symbol, value.var = "close")

# Keep only symbols with almost no NA values
keep_cols = names(which(colMeans(!is.na(train_dt)) > 0.99))
train_dt = train_dt[, .SD, .SDcols = keep_cols]

# Convert to xts and take logs
train = as.xts.data.table(train_dt) # convert to xts
train = train[, colSums(!is.na(train)) == max(colSums(!is.na(train)))]
train = log(train)

# Other parameteres
param_search_type = meta[i, search_type]
param_maxfact = meta[i, maxfact]


# COARSE BEST PAIRS -------------------------------------------------------
# choose best pairs using hedge.pci function and maxfact = 1 (only one factor possible)
pci_tests_i = list()
s = Sys.time()
for (j in 1:10) { # ncol(train)

  # DEBUG
  print(j)

  # quasi multivariate pairs
  hedge = tryCatch(hedge.pci(train[, j], train[, -j],
                             maxfact = param_maxfact,
                             use.multicore = FALSE,
                             search_type = param_search_type),
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
  names(metrics)[names(metrics) %in% names(hedge$pci$rho.se)] = "rho_se"
  names(metrics)[names(metrics) %in% names(hedge$pci$sigma_M.se)] = "sigma_M_se"
  names(metrics)[names(metrics) %in% names(hedge$pci$sigma_R.se)] = "sigma_R_se"
  names(metrics)[names(metrics) %in% names(hedge$pci$M0.se)] = "M0_se"
  names(metrics)[names(metrics) %in% names(hedge$pci$R0.se)] = "R0_se"

  # Convert to data.table, add resulsts to metricsa and p values
  metrics = as.data.table(as.list(metrics))
  results = cbind(results, metrics)

  pci_tests_i[[j]] = cbind(results,
                           p_rw = test_pci$p.value[1],
                           p_ar = test_pci$p.value[2])
}
e = Sys.time()
print(e - s)
pci_tests = rbindlist(pci_tests_i, fill = TRUE)

# Save
dir_ = "output_pci"
if (!dir.exists(dir_)) dir.create(dir_)
file_name = file.path("output_pci", paste0(i, ".csv"))
fwrite(pci_tests, file_name)
