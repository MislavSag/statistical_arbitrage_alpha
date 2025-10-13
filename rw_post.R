library(data.table)


# Setup
PATH_SAVE = "D:/strategies/statsarb"

# Import pairs
pairs = fread(file.path(PATH_SAVE, "pairs_combo.csv"))

# Filter pairs
pairs = pairs[
  same_sector == 1 &
    isFund1 == FALSE & isFund2 == FALSE &
    isEtf1 == FALSE & isEtf2 == FALSE &
    isin1 != isin2]

# Select pairs
cols = colnames(pairs)[grepl("^\\d+$", colnames(pairs))]
cols = c("stock1", "stock2", cols)
pairs = pairs[, ..cols]

# Reshape
pairs = melt(pairs, id.vars = c("stock1", "stock2"))
pairs = na.omit(pairs)
pairs[, q := as.numeric(paste0(substr(as.character(variable), 1, 4), ".", substr(as.character(variable), 5, 6)))]
pairs[, variable := NULL]
setorder(pairs, q, -value)

# Identify cheap stocks
pairs[value < 50]

