# Comparison Script: Original vs Factor-Adjusted Pairs Trading
# Run this AFTER both pairs_features.R and pairs_features_factor_adjusted.R

library(data.table)
library(arrow)
library(ggplot2)
library(patchwork)


# SETUP -------------------------------------------------------------------
PATH_SAVE = "H:/strategies/statsarb"


# LOAD DATA ---------------------------------------------------------------
cat("\n=== Loading Data for Comparison ===\n")

# Load original predictors
paths_original = list.files(file.path(PATH_SAVE, "predictors"), 
                             full.names = TRUE, pattern = "\\.feather$")
if (length(paths_original) > 0) {
  pairs_original = lapply(paths_original, read_feather)
  pairs_original = rbindlist(pairs_original)
  pairs_original[, method := "Original"]
  cat(sprintf("Original predictors: %d pairs across %d dates\n", 
              nrow(pairs_original), 
              pairs_original[, length(unique(date))]))
} else {
  cat("WARNING: No original predictors found in predictors/\n")
  pairs_original = NULL
}

# Load factor-adjusted predictors
paths_factor = list.files(file.path(PATH_SAVE, "predictors_factor_adj"), 
                          full.names = TRUE, pattern = "\\.feather$")
if (length(paths_factor) > 0) {
  pairs_factor = lapply(paths_factor, read_feather)
  pairs_factor = rbindlist(pairs_factor)
  pairs_factor[, method := "Factor-Adjusted"]
  cat(sprintf("Factor-adjusted predictors: %d pairs across %d dates\n",
              nrow(pairs_factor),
              pairs_factor[, length(unique(date))]))
} else {
  cat("WARNING: No factor-adjusted predictors found in predictors_factor_adj/\n")
  pairs_factor = NULL
}

# Check if we have both datasets
if (is.null(pairs_original) || is.null(pairs_factor)) {
  stop("Need both original and factor-adjusted predictors to compare!")
}


# COMPARE DISTRIBUTIONS ---------------------------------------------------
cat("\n=== Comparing Feature Distributions ===\n")

# Combine datasets
pairs_combined = rbind(pairs_original, pairs_factor)

# LSR comparison
cat("\n--- LSR Metric ---\n")
cat(sprintf("Original - Mean: %.4f, SD: %.4f\n",
            pairs_original[, mean(lsr, na.rm = TRUE)],
            pairs_original[, sd(lsr, na.rm = TRUE)]))
cat(sprintf("Factor-Adjusted - Mean: %.4f, SD: %.4f\n",
            pairs_factor[, mean(lsr, na.rm = TRUE)],
            pairs_factor[, sd(lsr, na.rm = TRUE)]))

# Distance comparison
cat("\n--- Distance Metric ---\n")
cat(sprintf("Original - Mean: %.4f, SD: %.4f\n",
            pairs_original[, mean(distance, na.rm = TRUE)],
            pairs_original[, sd(distance, na.rm = TRUE)]))
cat(sprintf("Factor-Adjusted - Mean: %.4f, SD: %.4f\n",
            pairs_factor[, mean(distance, na.rm = TRUE)],
            pairs_factor[, sd(distance, na.rm = TRUE)]))

# Ranked metrics comparison
cat("\n--- Ranked Metrics ---\n")
cat(sprintf("Original LSR bucket - Mean: %.4f, SD: %.4f\n",
            pairs_original[, mean(lsr_bucket, na.rm = TRUE)],
            pairs_original[, sd(lsr_bucket, na.rm = TRUE)]))
cat(sprintf("Factor-Adjusted LSR bucket - Mean: %.4f, SD: %.4f\n",
            pairs_factor[, mean(lsr_bucket, na.rm = TRUE)],
            pairs_factor[, sd(lsr_bucket, na.rm = TRUE)]))


# VISUALIZATION -----------------------------------------------------------
cat("\n=== Generating Comparison Plots ===\n")

# 1. LSR distribution comparison
p1 = ggplot(pairs_combined, aes(x = lsr, fill = method)) +
  geom_density(alpha = 0.5) +
  labs(title = "LSR Distribution: Original vs Factor-Adjusted",
       x = "LSR Value",
       y = "Density",
       fill = "Method") +
  theme_minimal() +
  theme(legend.position = "bottom")

# 2. Distance distribution comparison
p2 = ggplot(pairs_combined, aes(x = distance, fill = method)) +
  geom_density(alpha = 0.5) +
  xlim(0, quantile(pairs_combined$distance, 0.99, na.rm = TRUE)) +
  labs(title = "Distance Distribution: Original vs Factor-Adjusted",
       x = "Distance Value",
       y = "Density",
       fill = "Method") +
  theme_minimal() +
  theme(legend.position = "bottom")

# 3. LSR bucket comparison
p3 = ggplot(pairs_combined, aes(x = lsr_bucket, fill = method)) +
  geom_histogram(alpha = 0.5, bins = 50, position = "identity") +
  labs(title = "LSR Bucket Distribution: Original vs Factor-Adjusted",
       x = "LSR Bucket (Rank)",
       y = "Count",
       fill = "Method") +
  theme_minimal() +
  theme(legend.position = "bottom")

# 4. Distance rank comparison
p4 = ggplot(pairs_combined, aes(x = ed_rank, fill = method)) +
  geom_histogram(alpha = 0.5, bins = 50, position = "identity") +
  labs(title = "Distance Rank Distribution: Original vs Factor-Adjusted",
       x = "Distance Rank",
       y = "Count",
       fill = "Method") +
  theme_minimal() +
  theme(legend.position = "bottom")

# Combine plots
p_combined = (p1 + p2) / (p3 + p4)

# Save plot
ggsave(file.path(PATH_SAVE, "comparison_distributions.png"), p_combined,
       width = 14, height = 10, dpi = 150)
cat("Distribution comparison plot saved\n")


# PAIR-BY-PAIR COMPARISON -------------------------------------------------
cat("\n=== Pair-by-Pair Comparison ===\n")

# Merge datasets for same pairs on same dates
pairs_merged = merge(
  pairs_original[, .(stock1, stock2, date, 
                     lsr_orig = lsr, 
                     distance_orig = distance,
                     lsr_bucket_orig = lsr_bucket,
                     ed_rank_orig = ed_rank)],
  pairs_factor[, .(stock1, stock2, date,
                   lsr_factor = lsr,
                   distance_factor = distance,
                   lsr_bucket_factor = lsr_bucket,
                   ed_rank_factor = ed_rank)],
  by = c("stock1", "stock2", "date"),
  all = FALSE
)

cat(sprintf("Matched %d pairs across both methods\n", nrow(pairs_merged)))

# Calculate correlations
cat("\n--- Feature Correlations Between Methods ---\n")
cat(sprintf("LSR correlation: %.4f\n",
            cor(pairs_merged$lsr_orig, pairs_merged$lsr_factor, use = "complete.obs")))
cat(sprintf("Distance correlation: %.4f\n",
            cor(pairs_merged$distance_orig, pairs_merged$distance_factor, use = "complete.obs")))
cat(sprintf("LSR bucket correlation: %.4f\n",
            cor(pairs_merged$lsr_bucket_orig, pairs_merged$lsr_bucket_factor, use = "complete.obs")))
cat(sprintf("Distance rank correlation: %.4f\n",
            cor(pairs_merged$ed_rank_orig, pairs_merged$ed_rank_factor, use = "complete.obs")))

# Scatter plots
p5 = ggplot(pairs_merged, aes(x = lsr_orig, y = lsr_factor)) +
  geom_point(alpha = 0.1, size = 0.5) +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
  labs(title = "LSR: Original vs Factor-Adjusted",
       x = "Original LSR",
       y = "Factor-Adjusted LSR") +
  theme_minimal()

p6 = ggplot(pairs_merged, aes(x = lsr_bucket_orig, y = lsr_bucket_factor)) +
  geom_point(alpha = 0.1, size = 0.5) +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
  labs(title = "LSR Bucket: Original vs Factor-Adjusted",
       x = "Original LSR Bucket",
       y = "Factor-Adjusted LSR Bucket") +
  theme_minimal()

p_scatter = p5 + p6
ggsave(file.path(PATH_SAVE, "comparison_scatter.png"), p_scatter,
       width = 12, height = 6, dpi = 150)
cat("Scatter plot comparison saved\n")


# TOP PAIRS COMPARISON ----------------------------------------------------
cat("\n=== Top Pairs Comparison ===\n")

# Get top 100 pairs from each method (most recent date)
latest_date = max(pairs_merged$date)
top_original = pairs_merged[date == latest_date][order(lsr_bucket_orig)][1:100]
top_factor = pairs_merged[date == latest_date][order(lsr_bucket_factor)][1:100]

# How many overlap?
overlap = intersect(
  paste(top_original$stock1, top_original$stock2),
  paste(top_factor$stock1, top_factor$stock2)
)

cat(sprintf("\nTop 100 pairs on %s:\n", latest_date))
cat(sprintf("  Overlap: %d pairs (%.1f%%)\n", 
            length(overlap), 
            100 * length(overlap) / 100))
cat(sprintf("  Different: %d pairs\n", 100 - length(overlap)))


# RANKING CHANGES ---------------------------------------------------------
cat("\n=== Ranking Changes ===\n")

# Calculate rank changes for all pairs
pairs_merged[, rank_change_lsr := abs(lsr_bucket_orig - lsr_bucket_factor)]
pairs_merged[, rank_change_dist := abs(ed_rank_orig - ed_rank_factor)]

cat(sprintf("Mean LSR rank change: %.4f\n", 
            mean(pairs_merged$rank_change_lsr, na.rm = TRUE)))
cat(sprintf("Mean Distance rank change: %.4f\n",
            mean(pairs_merged$rank_change_dist, na.rm = TRUE)))
cat(sprintf("Pairs with >0.1 LSR rank change: %.1f%%\n",
            100 * mean(pairs_merged$rank_change_lsr > 0.1, na.rm = TRUE)))

# Plot rank changes over time
p7 = pairs_merged[, .(
  mean_rank_change_lsr = mean(rank_change_lsr, na.rm = TRUE),
  mean_rank_change_dist = mean(rank_change_dist, na.rm = TRUE)
), by = date] |>
  melt(id.vars = "date", variable.name = "metric", value.name = "rank_change") |>
  ggplot(aes(x = date, y = rank_change, color = metric)) +
  geom_line() +
  labs(title = "Mean Rank Changes Over Time",
       x = "Date",
       y = "Mean Absolute Rank Change",
       color = "Metric") +
  theme_minimal()

ggsave(file.path(PATH_SAVE, "comparison_rank_changes.png"), p7,
       width = 10, height = 6, dpi = 150)
cat("Rank changes plot saved\n")


# SUMMARY REPORT ----------------------------------------------------------
cat("\n", rep("=", 70), "\n", sep = "")
cat("COMPARISON SUMMARY: Original vs Factor-Adjusted\n")
cat(rep("=", 70), "\n\n", sep = "")

cat("Dataset Sizes:\n")
cat(sprintf("  Original: %d pair-dates\n", nrow(pairs_original)))
cat(sprintf("  Factor-Adjusted: %d pair-dates\n", nrow(pairs_factor)))
cat(sprintf("  Matched: %d pair-dates\n", nrow(pairs_merged)))

cat("\nKey Differences:\n")
cat(sprintf("  LSR correlation: %.3f\n",
            cor(pairs_merged$lsr_orig, pairs_merged$lsr_factor, use = "complete.obs")))
cat(sprintf("  Mean LSR change: %.4f\n",
            mean(abs(pairs_merged$lsr_orig - pairs_merged$lsr_factor), na.rm = TRUE)))
cat(sprintf("  Mean rank change: %.4f\n",
            mean(pairs_merged$rank_change_lsr, na.rm = TRUE)))
cat(sprintf("  Top 100 overlap: %.1f%%\n",
            100 * length(overlap) / 100))

cat("\nInterpretation:\n")
if (cor(pairs_merged$lsr_orig, pairs_merged$lsr_factor, use = "complete.obs") > 0.7) {
  cat("  HIGH correlation: Both methods produce similar rankings\n")
  cat("  Factor adjustment provides refinement rather than revolution\n")
} else {
  cat("  LOW correlation: Methods produce different rankings\n")
  cat("  Factor adjustment changes pair selection significantly\n")
}

cat("\nFiles Generated:\n")
cat("  - comparison_distributions.png\n")
cat("  - comparison_scatter.png\n")
cat("  - comparison_rank_changes.png\n")

cat("\nRecommendation:\n")
if (mean(pairs_merged$rank_change_lsr, na.rm = TRUE) > 0.2) {
  cat("  SIGNIFICANT rank changes detected\n")
  cat("  Factor adjustment makes a material difference\n")
  cat("  → Recommend using factor-adjusted version for trading\n")
} else {
  cat("  MODERATE rank changes detected\n")
  cat("  Both methods produce similar results\n")
  cat("  → Compare backtest performance before deciding\n")
}

cat("\n", rep("=", 70), "\n", sep = "")
