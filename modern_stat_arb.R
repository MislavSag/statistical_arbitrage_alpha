library(data.table)
library(finutils)


# Config
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




# --- SETUP: Replace this with your actual data loading ---
# We assume 'banks_rets' is a data.frame where the first column is the Date/Index
# and subsequent columns are the returns of the banks.

# 1. Create Sample Data.table (Replace with your actual data)
dates <- seq(as.Date("2024-01-01"), by = "day", length.out = 10)
banks_rets_dt <- data.table(
  Date = dates,
  BankA = rnorm(10, 0.001, 0.01),
  BankB = rnorm(10, 0.001, 0.01),
  BankC = rnorm(10, 0.001, 0.01)
)

# Create column names for the banks
bank_cols <- names(banks_rets_dt)[-1]

# Apply frollsum to each bank column and create new rolling sum columns (e.g., RS_BankA, RS_BankB)
banks_rets_dt[, paste0("RS_", bank_cols) := lapply(.SD, frollsum, n = 2), .SDcols = bank_cols]

# 2. De-mean the Rolling Sum (Subtract the row mean, axis=1)

# Identify the rolling sum columns
rolling_sum_cols <- grep("^RS_", names(banks_rets_dt), value = TRUE)

# Calculate the cross-sectional mean (Row Mean) of the rolling sums
banks_rets_dt[, CrossSecMean := rowMeans(.SD, na.rm = TRUE), .SDcols = rolling_sum_cols]

# De-mean the rolling sums and negate the result to get the intermediate signal:
# banks_rets_signal_intermediate = -(rolling_sum - CrossSecMean)

# Create the signal columns (e.g., S_BankA, S_BankB)
banks_rets_dt[, paste0("S_", bank_cols) := mapply(function(rs_col) {
  # rs_col is the rolling sum column name (e.g., "RS_BankA")
  # Subtract the CrossSecMean from the rolling sum and negate
  - (get(rs_col) - CrossSecMean)
}, rs_col = rolling_sum_cols, SIMPLIFY = FALSE)]

# 3. Normalize the Signal (Divide by the row's absolute sum)

# Identify the intermediate signal columns
signal_cols <- grep("^S_", names(banks_rets_dt), value = TRUE)

# Calculate the cross-sectional absolute sum of the intermediate signals
banks_rets_dt[, CrossSecAbsSum := rowSums(abs(.SD), na.rm = TRUE), .SDcols = signal_cols]

# Normalize the signal by dividing each row by the CrossSecAbsSum
# The result columns overwrite the intermediate signal columns (S_BankA, etc.)
banks_rets_dt[, (signal_cols) := mapply(function(s_col) {
  get(s_col) / CrossSecAbsSum
}, s_col = signal_cols, SIMPLIFY = FALSE)]

# The final signal is now in the columns S_BankA, S_BankB, S_BankC, etc.
banks_rets_signal <- banks_rets_dt[, .SD, .SDcols = c("Date", signal_cols)]

# Print the resulting signal data.table (first few rows)
print(head(banks_rets_signal))

# install.packages(c("ggplot2", "tidyr")) # Uncomment and run if you don't have them
library(ggplot2)
# The 'melt' function is from 'data.table' and is very efficient
banks_rets_signal_long <- melt(banks_rets_signal, 
                               id.vars = "Date", 
                               measure.vars = signal_cols, 
                               variable.name = "Bank", 
                               value.name = "Signal")

# Plot the result
ggplot(banks_rets_signal_long, aes(x = Date, y = Signal, color = Bank)) +
  geom_line() +
  labs(title = 'Banks 2D Returns Space Signal', y = 'Signal') +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5), # Center the title
        panel.grid.major = element_line(linetype = 'dotted'),
        panel.grid.minor = element_line(linetype = 'dotted'))