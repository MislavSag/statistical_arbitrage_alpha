library(fastverse)
library(finutils)
library(arrow)
library(roll)


# SETUP -------------------------------------------------------------------
PATH_SAVE = "H:/strategies/statsarb"
PATH_LEAN = "C:/Users/Mislav/qc_snp/data"
PATH_DATA = "F:/data/equity/us"


# DATA --------------------------------------------------------------------
# Import daily data (same as pairs_features.R)
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
  profiles_fmp_file = "F:/data/equity/us/fundamentals/prfiles.parquet",
  min_last_mon_mcap = file.path(PATH_DATA, "fundamentals", "market_cap.parquet")
)

# Import profiles for industry/sector info
profile = read_parquet("F:/data/equity/us/fundamentals/prfiles.parquet")
profile = unique(profile[, .(fmp_symbol = symbol, industry, sector)])
prices = profile[prices, on = c(fmp_symbol = "symbol")]


# FACTOR PREMIUMS ---------------------------------------------------------
# Calculate factor premiums using ALL stocks

# 1. MARKET FACTOR (equal-weighted market return)
factor_mkt = prices[, .(
  mkt_ret = mean(returns, na.rm = TRUE),
  mkt_ret_vw = weighted.mean(returns, market_cap, na.rm = TRUE)  # Value-weighted
), by = date]

# 2. SIZE FACTOR (SMB - Small Minus Big)
# Calculate size quintiles each month
prices[, q := data.table::yearmon(date)]
prices[, size_quintile := ntile(market_cap, 5), by = q]

factor_size = prices[!is.na(size_quintile), .(
  smb = mean(returns[size_quintile <= 2], na.rm = TRUE) - 
        mean(returns[size_quintile >= 4], na.rm = TRUE)
), by = date]

# 3. MOMENTUM FACTOR (WML - Winners Minus Losers)
# Calculate 12-month momentum (skipping most recent month)
prices[, mom_12m := frollapply(
  shift(returns, 21), 
  252, 
  sum, 
  na.rm = TRUE, 
  align = "right"
), by = symbol]

factor_mom = prices[!is.na(mom_12m), {
  mom_q90 = quantile(mom_12m, 0.9, na.rm = TRUE)
  mom_q10 = quantile(mom_12m, 0.1, na.rm = TRUE)
  
  .(wml = mean(returns[mom_12m > mom_q90], na.rm = TRUE) -
          mean(returns[mom_12m < mom_q10], na.rm = TRUE))
}, by = date]

# 4. SECTOR FACTORS (sector-specific returns)
factor_sector = prices[, .(
  sector_ret = mean(returns, na.rm = TRUE)
), by = .(date, sector)]

# 5. INDUSTRY FACTORS (industry-specific returns)
factor_industry = prices[, .(
  industry_ret = mean(returns, na.rm = TRUE)
), by = .(date, industry)]

# Combine all factor premiums
factors = factor_mkt[factor_size, on = "date"][factor_mom, on = "date"]


# FACTOR LOADINGS (BETAS) -------------------------------------------------
# Estimate factor loadings for each stock using rolling regression

# Merge factors with prices
prices_with_factors = factors[prices, on = "date"]

# Calculate rolling betas (252-day window)
stock_betas = prices_with_factors[, {
  if (.N < 252) {
    list(
      beta_mkt = NA_real_,
      beta_smb = NA_real_,
      beta_wml = NA_real_,
      alpha = NA_real_
    )
  } else {
    # Use last 252 observations for beta estimation
    data_window = tail(.SD, 252)
    
    # Multiple regression: returns ~ mkt + smb + wml
    model = lm(returns ~ mkt_ret + smb + wml, 
               data = data_window, 
               na.action = na.exclude)
    
    list(
      beta_mkt = coef(model)["mkt_ret"],
      beta_smb = coef(model)["smb"],
      beta_wml = coef(model)["wml"],
      alpha = coef(model)["(Intercept)"]
    )
  }
}, by = .(symbol = fmp_symbol, date)]

# Alternative: Rolling regression for time-varying betas
stock_betas_rolling = prices_with_factors[order(fmp_symbol, date), {
  n = .N
  if (n < 252) {
    data.table(
      date = date,
      beta_mkt = NA_real_,
      beta_smb = NA_real_,
      beta_wml = NA_real_
    )
  } else {
    # Rolling regression using roll package
    y = returns
    X = cbind(1, mkt_ret, smb, wml)  # Include intercept
    
    betas = roll_lm(x = X, y = y, width = 252, min_obs = 200)
    
    data.table(
      date = date,
      beta_mkt = betas$coefficients[, "mkt_ret"],
      beta_smb = betas$coefficients[, "smb"],
      beta_wml = betas$coefficients[, "wml"]
    )
  }
}, by = .(symbol = fmp_symbol)]


# IDIOSYNCRATIC RETURNS ---------------------------------------------------
# Calculate idiosyncratic returns (residuals after removing factor exposure)

prices_with_betas = stock_betas_rolling[prices_with_factors, 
                                         on = .(symbol = fmp_symbol, date)]

# Idiosyncratic return = actual return - factor-explained return
prices_with_betas[, ret_idio := returns - 
                    (beta_mkt * mkt_ret + 
                     beta_smb * smb + 
                     beta_wml * wml)]

# Industry-adjusted returns (simpler alternative)
prices_with_betas[factor_industry, 
                  ret_industry_adj := returns - i.industry_ret,
                  on = .(date, industry)]

# Sector-adjusted returns (even simpler)
prices_with_betas[factor_sector,
                  ret_sector_adj := returns - i.sector_ret,
                  on = .(date, sector)]


# SAVE RESULTS ------------------------------------------------------------
# Save factor premiums
write_feather(factors, file.path(PATH_SAVE, "factor_premiums.feather"))

# Save stock betas
write_feather(stock_betas_rolling, file.path(PATH_SAVE, "stock_betas.feather"))

# Save prices with idiosyncratic returns
prices_adjusted = prices_with_betas[, .(
  fmp_symbol = symbol,
  date,
  close,
  returns,
  ret_idio,           # Full factor-adjusted
  ret_industry_adj,   # Industry-adjusted only
  ret_sector_adj,     # Sector-adjusted only
  beta_mkt,
  beta_smb,
  beta_wml
)]

write_feather(prices_adjusted, file.path(PATH_SAVE, "prices_adjusted.feather"))

cat("\n=== Factor Analysis Complete ===\n")
cat(sprintf("Factor premiums saved: %d dates\n", factors[, .N]))
cat(sprintf("Stock betas saved: %d stock-dates\n", stock_betas_rolling[, .N]))
cat(sprintf("Adjusted returns saved: %d observations\n", prices_adjusted[, .N]))

# Diagnostic plots
library(ggplot2)

# Plot factor premiums over time
factors_long = melt(factors[, .(date, mkt_ret, smb, wml)], 
                    id.vars = "date",
                    variable.name = "factor",
                    value.name = "premium")

ggplot(factors_long, aes(x = date, y = premium, color = factor)) +
  geom_line() +
  facet_wrap(~factor, scales = "free_y", ncol = 1) +
  labs(title = "Factor Premiums Over Time",
       y = "Daily Return") +
  theme_minimal()

# Check correlation between factors
cat("\n=== Factor Correlation Matrix ===\n")
print(cor(factors[, .(mkt_ret, smb, wml)], use = "complete.obs"))

# Distribution of idiosyncratic returns
cat("\n=== Idiosyncratic Return Statistics ===\n")
cat(sprintf("Mean: %.6f\n", mean(prices_adjusted$ret_idio, na.rm = TRUE)))
cat(sprintf("SD:   %.6f\n", sd(prices_adjusted$ret_idio, na.rm = TRUE)))
cat(sprintf("Compared to raw return SD: %.6f\n", 
            sd(prices_adjusted$returns, na.rm = TRUE)))
cat(sprintf("Variance explained by factors: %.1f%%\n",
            (1 - var(prices_adjusted$ret_idio, na.rm = TRUE) / 
                 var(prices_adjusted$returns, na.rm = TRUE)) * 100))
