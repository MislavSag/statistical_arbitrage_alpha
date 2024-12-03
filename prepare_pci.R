library(data.table)


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

# Add to padobran
fwrite(meta, "F:/strategies/statsarb/pci/meta.csv")
fwrite(prices, "F:/strategies/statsarb/pci/prices.csv")
# scp F:/strategies/statsarb/pci/meta.csv padobran:/home/jmaric/predictors_padobran/
# scp /home/sn/data/strategies/pread/ padobran:/home/jmaric/pread/predictors_padobran/
file.remove("F:/strategies/statsarb/pci/meta.csv")
file.remove("F:/strategies/statsarb/pci/prices.csv")


# SH FILE -----------------------------------------------------------------
# Create sh file for padobran with PBS j equal to number of rows
sh_file = sprintf("
#!/bin/bash

#PBS -N STATSARBPCI
#PBS -l ncpus=2
#PBS -l mem=6GB
#PBS -J 1-%d
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif estimate_pci_padobran.R
", nrow(meta))
sh_file_name = "estimate_pci_padobran.sh"
file.create(sh_file_name)
writeLines(sh_file, sh_file_name)
