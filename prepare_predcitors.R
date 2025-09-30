library(qlcal)
library(arrow)
library(data.table)
library(finutils)
library(finfeatures)


# SET UP ------------------------------------------------------------------
# set calendar
qlcal::setCalendar("UnitedStates/NYSE")

# Paths
URIFACTORS = "F:/data/equity/us/predictors_daily/factors"
PREDICTORS_SAVE = "D:/strategies/statsarb"
list.files(URIFACTORS)


# PREDICTORS ---------------------------------------------------------------
# Fundamental predictors
fundamentals = read_parquet(file.path(URIFACTORS, "fundamental_factors.parquet"))

# Prices
prices = qc_daily_parquet(
  file_path = "F:/lean/data/all_stocks_daily",
  min_obs = 1100,
  duplicates = "fast"
)
prices[, month := data.table::yearmon(date)]

# Predictors
prices[, index := date == data.table::last(date), by = .(symbol, month)]
at_ = prices[, which(index)]
prices[, c("index") := NULL]
predictors_ohlcv = OhlcvFeaturesDaily$new(
  at = at_,
  windows = c(5, 10, 22, 66, 125, 252, 500, 1000),
  quantile_divergence_window = c(22, 66, 125, 252, 500, 1000)
)
predictors = predictors_ohlcv$get_ohlcv_features(copy(prices))

# Save predictors
fwrite(predictors, file.path(PREDICTORS_SAVE, "predctors.csv"))
