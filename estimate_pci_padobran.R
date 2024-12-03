library(data.table)
library(xts)
library(partialCI)


# PARAMS ------------------------------------------------------------------
# Parameters
FREQ = "m" # Frequency - for now I would suggest month (m) and week (w)


# PRICE DATA --------------------------------------------------------------
# Import QC daily data
prices = fread("F:/lean/data/stocks_daily.csv")
setnames(prices, gsub(" ", "_", c(tolower(colnames(prices)))))

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Remove duplicates - there are same for different symbols (eg. phun and phun.1)
dups = prices[, .(symbol , n = .N),
              by = .(date, open, high, low, close, volume, adj_close,
                     symbol_first = substr(symbol, 1, 1))]
dups = dups[n > 1]
dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
symbols_remove = dups[, .(symbol, n = .N),
                      by = .(date, open, high, low, close, volume, adj_close,
                             symbol_short)]
symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
prices = prices[symbol %notin% symbols_remove]

# Adjust all columns
prices[, adj_rate := adj_close / close]
prices[, let(
  open = open*adj_rate,
  high = high*adj_rate,
  low = low*adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, let(adj_rate = NULL)]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Remove observations where open, high, low, close columns are below 1e-008
# This step is opional, we need it if we will use finfeatures package
prices = prices[open > 1e-008 & high > 1e-008 & low > 1e-008 & close > 1e-008]

# Remove missing values
prices = na.omit(prices)

# Keep only symbol with at least 2 years of data
# This step is optional
symbol_keep = prices[, .N, symbol][N >= 2 * 252, symbol]
prices = prices[symbol %chin% symbol_keep]

# Sort
setorder(prices, symbol, date)

# free memory
gc()


# FILTERING ---------------------------------------------------------------
# Add label for most liquid asssets
prices[, dollar_volume_month := frollsum(close_raw * volume, 22, na.rm= TRUE), by = symbol]
calculate_liquid = function(prices, n) {
  # dt = copy(prices)
  # n = 500
  dt = copy(prices)
  setorder(dt, date, -dollar_volume_month)
  filtered = na.omit(dt)[, .(symbol = first(symbol, n)), by = date]
  col_ = paste0("liquid_", n)
  filtered[, (col_) := TRUE]
  dt = filtered[dt, on = c("date", "symbol")]
  dt[is.na(x), x := FALSE, env = list(x = col_)] # fill NA with FALSE
  return(dt)
}
prices = calculate_liquid(prices, 100)
prices = calculate_liquid(prices, 200)
prices = calculate_liquid(prices, 500)
prices = calculate_liquid(prices, 1000)

# Remove columns we don't need
prices[, dollar_volume_month := NULL]

# Order again
setorder(prices, symbol, date)


# PARAMETERS --------------------------------------------------------------
# Define parameters
train_size_years = c(2, 5, 10)

# Create sh file with one node for every parameter
dates = seq.Date(prices[, min(date)], prices[, max(date)], by = "months")

# Util function that creates train and test sets
create_rolling_splits = function(dates, train_size = 12, test_size = 1) {
  # Ensure the dates are sorted
  dates = sort(dates)
  dates_dt = data.table(date = dates)

  # Initialize a list to store the train-test splits
  splits_list = list()

  total_periods = nrow(dates_dt)

  # Loop over each possible split point
  for (i in seq(train_size + 1, total_periods - test_size + 1)) {
    # Training indices
    train_indices = seq(i - train_size, i - 1)

    # Testing indices
    test_indices = seq(i, i + test_size - 1)

    # Store the indices in the splits list
    splits_list[[length(splits_list) + 1]] = data.table(
      train_size = train_size,
      test_size = test_size,
      train_start = dates[first(train_indices)],
      train_end = dates[last(train_indices)],
      test_start = dates[first(test_indices)],
      test_end = dates[last(test_indices)]
    )
  }

  rbindlist(splits_list)
}

# Create train/test splits
meta = lapply(train_size_years, function(x) create_rolling_splits(dates, x*12, 1))
meta = rbindlist(meta)

# Add search_type and maxfact option from hedge.pci
CJ_table_1 = function(X,Y) setkey(X[,c(k=1,.SD)],k)[Y[,c(k=1,.SD)],allow.cartesian=TRUE][,k:=NULL]
meta = CJ_table_1(meta, data.table(search_type = c("lasso", "limited")))
meta = CJ_table_1(meta, data.table(maxfact = 1:2))


# PREPARE DATA ------------------------------------------------------------
# i'th row from meta
i = 2000

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
for (j in 1:10) { # 1:ncol(train)

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
