# Validation Script for Factor Adjustment
# Run this to compare raw vs adjusted spreads

library(data.table)
library(ggplot2)
library(patchwork)
library(tseries)

# Load a sample of pairs data (after running pairs_features.R with new code)
# This assumes you've saved intermediate results for testing

# Example validation for a single pair
validate_single_pair = function(dt_, stock1_sym, stock2_sym) {
  
  pair_data = dt_[stock1 == stock1_sym & stock2 == stock2_sym]
  
  if (nrow(pair_data) == 0) {
    print(paste("No data for pair:", stock1_sym, "-", stock2_sym))
    return(NULL)
  }
  
  # 1. Plot raw vs adjusted spreads
  p1 = ggplot(pair_data, aes(x = date)) +
    geom_line(aes(y = spreadclose_raw, color = "Raw Spread")) +
    geom_line(aes(y = spreadclose, color = "Adjusted Spread")) +
    labs(title = paste("Spread Comparison:", stock1_sym, "-", stock2_sym),
         y = "Log Spread", color = "Type") +
    theme_minimal()
  
  # 2. Plot market factor impact
  p2 = ggplot(pair_data, aes(x = date, y = market_factor)) +
    geom_line(color = "darkred") +
    geom_hline(yintercept = 0, linetype = "dashed") +
    labs(title = "Market Factor Removed", y = "Market Factor") +
    theme_minimal()
  
  # 3. Correlation analysis
  spread_diff_raw = diff(pair_data$spreadclose_raw)
  spread_diff_adj = diff(pair_data$spreadclose)
  market_ret = pair_data$market_factor[-1]
  
  cor_raw = cor(spread_diff_raw, market_ret, use = "complete.obs")
  cor_adj = cor(spread_diff_adj, market_ret, use = "complete.obs")
  
  cat("\n=== Correlation with Market Factor ===\n")
  cat(sprintf("Raw spread:      %.4f\n", cor_raw))
  cat(sprintf("Adjusted spread: %.4f\n", cor_adj))
  cat(sprintf("Reduction:       %.1f%%\n", (1 - abs(cor_adj)/abs(cor_raw)) * 100))
  
  # 4. Stationarity test (ADF test)
  adf_raw = adf.test(na.omit(pair_data$spreadclose_raw))
  adf_adj = adf.test(na.omit(pair_data$spreadclose))
  
  cat("\n=== Stationarity (ADF Test) ===\n")
  cat(sprintf("Raw spread p-value:      %.4f %s\n", 
              adf_raw$p.value, 
              ifelse(adf_raw$p.value < 0.05, "(Stationary)", "(Non-stationary)")))
  cat(sprintf("Adjusted spread p-value: %.4f %s\n", 
              adf_adj$p.value,
              ifelse(adf_adj$p.value < 0.05, "(Stationary)", "(Non-stationary)")))
  
  # 5. Variance comparison
  var_raw = var(pair_data$spreadclose_raw, na.rm = TRUE)
  var_adj = var(pair_data$spreadclose, na.rm = TRUE)
  
  cat("\n=== Spread Variance ===\n")
  cat(sprintf("Raw spread:      %.6f\n", var_raw))
  cat(sprintf("Adjusted spread: %.6f\n", var_adj))
  cat(sprintf("Change:          %.1f%%\n", (var_adj/var_raw - 1) * 100))
  
  # 6. Half-life of mean reversion
  compute_half_life = function(spread) {
    spread_lag = c(NA, spread[-length(spread)])
    model = lm(diff(spread) ~ spread_lag[-1], na.action = na.omit)
    lambda = coef(model)[2]
    half_life = -log(2) / lambda
    return(half_life)
  }
  
  hl_raw = compute_half_life(pair_data$spreadclose_raw)
  hl_adj = compute_half_life(pair_data$spreadclose)
  
  cat("\n=== Mean Reversion Speed (Half-Life in days) ===\n")
  cat(sprintf("Raw spread:      %.1f days\n", hl_raw))
  cat(sprintf("Adjusted spread: %.1f days\n", hl_adj))
  cat(sprintf("Change:          %.1f days (%.1f%%)\n", 
              hl_adj - hl_raw, 
              (hl_adj/hl_raw - 1) * 100))
  
  # Return plots
  print(p1 / p2)
  
  return(list(
    cor_reduction = (1 - abs(cor_adj)/abs(cor_raw)) * 100,
    stationary_improved = adf_adj$p.value < adf_raw$p.value,
    variance_change = (var_adj/var_adj - 1) * 100,
    halflife_raw = hl_raw,
    halflife_adj = hl_adj
  ))
}

# Aggregate validation across multiple pairs
validate_multiple_pairs = function(dt_, n_pairs = 50) {
  
  # Sample random pairs
  unique_pairs = unique(dt_[, .(stock1, stock2)])
  sample_pairs = unique_pairs[sample(.N, min(n_pairs, .N))]
  
  results = list()
  
  for (i in 1:nrow(sample_pairs)) {
    s1 = sample_pairs[i, stock1]
    s2 = sample_pairs[i, stock2]
    
    pair_data = dt_[stock1 == s1 & stock2 == s2]
    
    if (nrow(pair_data) < 100) next
    
    # Calculate metrics
    spread_diff_raw = diff(pair_data$spreadclose_raw)
    spread_diff_adj = diff(pair_data$spreadclose)
    market_ret = pair_data$market_factor[-1]
    
    cor_raw = cor(spread_diff_raw, market_ret, use = "complete.obs")
    cor_adj = cor(spread_diff_adj, market_ret, use = "complete.obs")
    
    var_raw = var(pair_data$spreadclose_raw, na.rm = TRUE)
    var_adj = var(pair_data$spreadclose, na.rm = TRUE)
    
    results[[i]] = data.table(
      pair = paste(s1, s2, sep = "-"),
      cor_raw = cor_raw,
      cor_adj = cor_adj,
      cor_reduction = abs(cor_adj) / abs(cor_raw),
      var_raw = var_raw,
      var_adj = var_adj,
      var_ratio = var_adj / var_raw
    )
  }
  
  results_dt = rbindlist(results)
  
  # Summary statistics
  cat("\n=== AGGREGATE VALIDATION SUMMARY ===\n")
  cat(sprintf("Number of pairs analyzed: %d\n", nrow(results_dt)))
  cat("\n--- Market Correlation Reduction ---\n")
  cat(sprintf("Mean correlation reduction: %.1f%%\n", 
              (1 - mean(results_dt$cor_reduction, na.rm = TRUE)) * 100))
  cat(sprintf("Median correlation reduction: %.1f%%\n",
              (1 - median(results_dt$cor_reduction, na.rm = TRUE)) * 100))
  cat(sprintf("Pairs with reduced correlation: %.1f%%\n",
              mean(results_dt$cor_reduction < 1, na.rm = TRUE) * 100))
  
  cat("\n--- Variance Changes ---\n")
  cat(sprintf("Mean variance change: %.1f%%\n",
              (mean(results_dt$var_ratio, na.rm = TRUE) - 1) * 100))
  cat(sprintf("Median variance change: %.1f%%\n",
              (median(results_dt$var_ratio, na.rm = TRUE) - 1) * 100))
  
  # Plots
  p1 = ggplot(results_dt, aes(x = cor_reduction)) +
    geom_histogram(bins = 30, fill = "steelblue", alpha = 0.7) +
    geom_vline(xintercept = 1, linetype = "dashed", color = "red") +
    labs(title = "Distribution of Correlation Reduction",
         x = "Adjusted Cor / Raw Cor (< 1 is improvement)",
         y = "Count") +
    theme_minimal()
  
  p2 = ggplot(results_dt, aes(x = var_ratio)) +
    geom_histogram(bins = 30, fill = "darkgreen", alpha = 0.7) +
    geom_vline(xintercept = 1, linetype = "dashed", color = "red") +
    labs(title = "Distribution of Variance Ratio",
         x = "Adjusted Var / Raw Var",
         y = "Count") +
    theme_minimal()
  
  print(p1 / p2)
  
  return(results_dt)
}

# Example usage (uncomment and modify as needed):
# 
# # After running pairs_features.R, you can test with:
# validate_single_pair(dt_, "AAPL", "MSFT")
# 
# # Or validate many pairs:
# validation_results = validate_multiple_pairs(dt_, n_pairs = 100)
# 
# # Save validation results
# fwrite(validation_results, "validation_results.csv")

cat("\n=== Validation Functions Loaded ===\n")
cat("Use: validate_single_pair(dt_, 'STOCK1', 'STOCK2')\n")
cat("Or:  validate_multiple_pairs(dt_, n_pairs = 50)\n\n")
