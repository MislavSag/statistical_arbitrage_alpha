library(data.table)
library(arrow)
library(roll)


# Set up
PATH = "H:/strategies/statsarb"

# Metadata for every stock
profile = read_parquet("F:/data/equity/us/fundamentals/prfiles.parquet")
profile = unique(profile[, .(fmp_symbol = symbol, industry, sector, isEtf, isFund, companyName)])

# Import data
paths = list.files(file.path(PATH, "predictors"), full.names = TRUE)
pairs = lapply(paths[1:20], read_feather)
pairs = rbindlist(pairs)

# Check unique dates
pairs[, unique(date)]
pairs[, length(unique(date))] == length(paths[1:12])
pairs[, hist(lsr_bucket)]
pairs[, hist(ed_rank)]

# Define weights for 12 months
width = 12
half_life = 3
lambda = 0.5^(1/half_life)
w = lambda^(width:1)

# Combined scores
setorder(pairs, stock1, stock2, date)
pairs[, roll_wmean_lsr := roll_mean(lsr_bucket, width, weights = w), by = .(stock1, stock2)]
pairs[, roll_wmean_ed  := roll_mean(ed_rank, width, weights = w),  by = .(stock1, stock2)]
pairs[, combo := (roll_wmean_lsr + roll_wmean_ed) / 2]

# Check number of NA's
pairs[, sum(is.na(combo)) / nrow(pairs) * 100]

# Remove missing values
pairs = na.omit(pairs)

# Add data from profiles
pairs = merge(pairs, profile, by.x = "stock1", by.y = "fmp_symbol", all.x = TRUE, all.y = FALSE)
cols = colnames(profile)[-1]
setnames(pairs, cols, paste0(cols, "1"))
pairs = merge(pairs, profile, by.x = "stock2", by.y = "fmp_symbol", all.x = TRUE, all.y = FALSE)
setnames(pairs, cols, paste0(cols, "2"))

# Save
write_feather(pairs, file.path(PATH, "pairs_combo.feather"))
