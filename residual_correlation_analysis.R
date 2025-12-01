library(data.table)
library(arrow)
library(ggplot2)
library(corrplot)


# SETUP -------------------------------------------------------------------
PATH_SAVE = "H:/strategies/statsarb"


# LOAD DATA ---------------------------------------------------------------
# Load factor-adjusted prices
prices_adjusted = read_feather(file.path(PATH_SAVE, "prices_adjusted.feather"))

# Load existing pairs
pairs = read_feather(file.path(PATH_SAVE, "pairs.feather"))


# RESIDUAL CORRELATION ANALYSIS -------------------------------------------
# Calculate correlation matrix of idiosyncratic returns
# This identifies which stocks move together AFTER removing common factors

cat("\n=== Calculating Residual Correlation Matrix ===\n")
cat("This may take several minutes for large datasets...\n")

# Select a specific time period for analysis (e.g., last 252 days)
analysis_dates = prices_adjusted[, tail(sort(unique(date)), 252)]
prices_window = prices_adjusted[date %in% analysis_dates]

# Pivot to wide format (stocks as columns, dates as rows)
returns_wide = dcast(
  prices_window[, .(date, fmp_symbol, ret_idio)],
  date ~ fmp_symbol,
  value.var = "ret_idio"
)

# Calculate correlation matrix
# Remove date column for correlation calculation
returns_matrix = as.matrix(returns_wide[, -1])
colnames(returns_matrix) = colnames(returns_wide)[-1]

# Calculate correlation (pairwise complete observations)
cat("Calculating correlation matrix...\n")
cor_matrix = cor(returns_matrix, use = "pairwise.complete.obs")

# Convert to long format for analysis
cor_long = melt(
  as.data.table(cor_matrix, keep.rownames = "stock1"),
  id.vars = "stock1",
  variable.name = "stock2",
  value.name = "residual_corr"
)
cor_long = cor_long[stock1 != stock2]  # Remove self-correlations
cor_long[, residual_corr_abs := abs(residual_corr)]


# ANALYZE CORRELATION DISTRIBUTION ----------------------------------------
cat("\n=== Residual Correlation Statistics ===\n")
cat(sprintf("Mean correlation: %.4f\n", 
            mean(cor_long$residual_corr, na.rm = TRUE)))
cat(sprintf("Median correlation: %.4f\n",
            median(cor_long$residual_corr, na.rm = TRUE)))
cat(sprintf("SD of correlations: %.4f\n",
            sd(cor_long$residual_corr, na.rm = TRUE)))
cat(sprintf("Max correlation: %.4f\n",
            max(cor_long$residual_corr, na.rm = TRUE)))
cat(sprintf("Min correlation: %.4f\n",
            min(cor_long$residual_corr, na.rm = TRUE)))

# Distribution plot
p1 = ggplot(cor_long, aes(x = residual_corr)) +
  geom_histogram(bins = 100, fill = "steelblue", alpha = 0.7) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  labs(
    title = "Distribution of Idiosyncratic Return Correlations",
    x = "Correlation",
    y = "Count"
  ) +
  theme_minimal()
print(p1)


# COMPARE WITH EXISTING PAIRS ---------------------------------------------
# Check if current pairs have high residual correlation

# Create pair identifiers (ensure alphabetical order for matching)
pairs[, pair_id := paste(pmin(stock1, stock2), pmax(stock1, stock2), sep = "_")]
cor_long[, pair_id := paste(pmin(stock1, stock2), pmax(stock1, stock2), sep = "_")]

# Mark existing pairs
cor_long[, is_existing_pair := pair_id %in% pairs$pair_id]

# Compare correlation for existing vs non-existing pairs
cat("\n=== Comparison: Existing Pairs vs All Pairs ===\n")
cat(sprintf("Existing pairs mean correlation: %.4f\n",
            cor_long[is_existing_pair == TRUE, mean(residual_corr, na.rm = TRUE)]))
cat(sprintf("Non-existing pairs mean correlation: %.4f\n",
            cor_long[is_existing_pair == FALSE, mean(residual_corr, na.rm = TRUE)]))
cat(sprintf("Difference: %.4f\n",
            cor_long[is_existing_pair == TRUE, mean(residual_corr, na.rm = TRUE)] -
            cor_long[is_existing_pair == FALSE, mean(residual_corr, na.rm = TRUE)]))

# Distribution comparison plot
p2 = ggplot(cor_long, aes(x = residual_corr, fill = is_existing_pair)) +
  geom_density(alpha = 0.5) +
  labs(
    title = "Residual Correlation: Existing Pairs vs All Pairs",
    x = "Correlation",
    y = "Density",
    fill = "Existing Pair"
  ) +
  theme_minimal()
print(p2)


# IDENTIFY HIGH-CORRELATION PAIRS -----------------------------------------
# Find pairs with highest idiosyncratic correlation
# These are good candidate pairs!

# Filter for high correlations
high_cor_threshold = 0.5  # Adjust as needed
high_cor_pairs = cor_long[residual_corr > high_cor_threshold]
setorder(high_cor_pairs, -residual_corr)

cat("\n=== Top 50 Pairs by Idiosyncratic Correlation ===\n")
print(head(high_cor_pairs[, .(stock1, stock2, residual_corr)], 50))

# Add sector/industry information
profile = read_parquet("F:/data/equity/us/fundamentals/prfiles.parquet")
profile = unique(profile[, .(fmp_symbol = symbol, industry, sector)])

high_cor_pairs = profile[high_cor_pairs, on = .(fmp_symbol = stock1)]
setnames(high_cor_pairs, c("industry", "sector"), c("industry1", "sector1"))
high_cor_pairs = profile[high_cor_pairs, on = .(fmp_symbol = stock2)]
setnames(high_cor_pairs, c("industry", "sector"), c("industry2", "sector2"))

# Check if high-correlation pairs are in same sector/industry
high_cor_pairs[, same_sector := sector1 == sector2]
high_cor_pairs[, same_industry := industry1 == industry2]

cat("\n=== Sector/Industry Analysis of High-Correlation Pairs ===\n")
cat(sprintf("Same sector: %.1f%%\n",
            high_cor_pairs[, mean(same_sector, na.rm = TRUE) * 100]))
cat(sprintf("Same industry: %.1f%%\n",
            high_cor_pairs[, mean(same_industry, na.rm = TRUE) * 100]))


# RECOMMENDATION: UPDATE PAIRS UNIVERSE -----------------------------------
# Suggest filtering pairs by minimum residual correlation

cat("\n=== RECOMMENDATION ===\n")
cat("Consider filtering pair universe by residual correlation.\n")
cat("Suggested thresholds:\n")
cat("  - Conservative: residual_corr > 0.6 (strong idiosyncratic linkage)\n")
cat("  - Moderate:     residual_corr > 0.5 (moderate linkage)\n")
cat("  - Aggressive:   residual_corr > 0.4 (weak but present linkage)\n\n")

# Count pairs at different thresholds
for (thresh in c(0.4, 0.5, 0.6, 0.7)) {
  n_pairs = cor_long[residual_corr > thresh, .N]
  cat(sprintf("Pairs with correlation > %.1f: %d\n", thresh, n_pairs))
}


# SAVE RESULTS ------------------------------------------------------------
# Save residual correlation matrix for use in pairs selection
write_feather(cor_long, file.path(PATH_SAVE, "residual_correlations.feather"))

# Save high-correlation pairs
write_feather(high_cor_pairs, file.path(PATH_SAVE, "high_correlation_pairs.feather"))

cat("\n=== Results Saved ===\n")
cat(sprintf("Residual correlations: %s\n", 
            file.path(PATH_SAVE, "residual_correlations.feather")))
cat(sprintf("High-correlation pairs: %s\n",
            file.path(PATH_SAVE, "high_correlation_pairs.feather")))


# OPTIONAL: CORRELATION HEATMAP -------------------------------------------
# Visualize correlation matrix for subset of stocks (too large for all stocks)

# Select top 50 stocks by trading volume or market cap
top_stocks = prices_adjusted[, .(
  avg_volume = mean(close * 1000000, na.rm = TRUE)  # Approximate volume
), by = fmp_symbol][order(-avg_volume)][1:50, fmp_symbol]

# Subset correlation matrix
cor_subset = cor_matrix[rownames(cor_matrix) %in% top_stocks,
                        colnames(cor_matrix) %in% top_stocks]

# Plot heatmap
png(file.path(PATH_SAVE, "residual_correlation_heatmap.png"), 
    width = 1200, height = 1200, res = 100)
corrplot(cor_subset, 
         method = "color",
         type = "upper",
         order = "hclust",
         tl.col = "black",
         tl.cex = 0.7,
         title = "Idiosyncratic Return Correlations (Top 50 Stocks)",
         mar = c(0, 0, 2, 0))
dev.off()

cat("\nCorrelation heatmap saved.\n")
