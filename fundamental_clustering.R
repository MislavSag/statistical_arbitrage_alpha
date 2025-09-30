# library(arrow)
# library(data.table)
# library(ggplot2)
# library(patchwork)
# library(qlcal)
# library(DescTools)
# library(mlr3finance)
# library(mlr3pipelines)
# library(PerformanceAnalytics)
# library(lubridate)
# library(AzureStor)

# library(mlr3cluster)
# library(mlr3viz)
# library(mlr3)
# library(lubridate)
# library(findata)
# library(cluster)
# library(RANN)



# SET UP ------------------------------------------------------------------
# set calendar
qlcal::setCalendar("UnitedStates/NYSE")

# Paths
URIFACTORS = "F:/data/equity/us/predictors_daily/factors"
list.files(URIFACTORS)


# PREDICTORS ---------------------------------------------------------------
# import factors
fundamentals = read_parquet(file.path(URIFACTORS, "fundamental_factors.parquet"))
cols = names(open_dataset(file.path(URIFACTORS, "prices_factors.parquet")))
prices = read_parquet(
  file.path(URIFACTORS, "prices_factors.parquet"),
  col_select = c("symbol", "date", "open", "close", "close_raw", "volume", "returns"))
macros = read_parquet(file.path(URIFACTORS, "macro_factors.parquet"))

# filter dates and symbols
# prices_dt <- price_factors[symbol %in% sp500_symbols]
prices = unique(prices, by = c("symbol", "date"))

# change date to data.table date
prices[, date := data.table::as.IDate(date)]

# order data
setkey(prices, symbol)
setorder(prices, "symbol", "date")

# remove symbols with less than 4 years of data
prices_n = prices[, .N, by = symbol]
prices_n = prices_n[which(prices_n$N > 252*4)]  # remove prices with only 700 or less observations
prices = prices[.(prices_n$symbol)]

# remove missing values
prices = na.omit(prices, cols = c("symbol", "date", "open", "close", "volume", "returns"))

# create year month id column
prices[, month := data.table::yearmon(date)]
prices[, .(date, month)]


# PREPARE DATA ------------------------------------------------------------
# downsample market data by lowering frequency to one month
dtm = prices[, .(
  date = data.table::last(date),
  open = data.table::first(open),
  close = data.table::last(close),
  close_raw = data.table::last(close_raw),
  volume = sum(volume, na.rm = TRUE)
), by = c("symbol", "month")]
setorder(dtm, symbol, date)

# Keep tradable
dtm = dtm[close_raw > 5]
dtm = dtm[volume > 500000]

# generate predictors
mom_size = 1:48
mom_cls = paste0("mom", mom_size, "m")
dtm[, .(symbol, date, close)]
dtm[, (mom_cls) := lapply(mom_size, function(x) close / shift(close, x) - 1), by = symbol]
dtm[, .(symbol, date, close, mom1m)]

# generate target - month return
dtm[, momret := close / open - 1]
dtm[, momret := shift(momret, 1, type = "lag"), by = symbol]
dtm[, .(symbol, date, mom1m, momret)]

# clean fundamentals
fundamentals = fundamentals[date > as.Date("2000-01-01")]
fundamentals[, acceptedDateFundamentals := acceptedDate]
setnames(fundamentals, "date", "fundamental_date")
fundamentals = unique(fundamentals, by = c("symbol", "acceptedDate"))

# merge price factors and fundamental data
dtm[, date_ := date]
fundamentals[, acceptedDate := as.IDate(acceptedDate)]
firms = fundamentals[dtm, , on = c("symbol", "acceptedDate" = "date_"), roll = Inf]
firms[, .(symbol, fundamental_date, date)]
firms[, `:=`(acceptedDateFundamentals = NULL, rd = NULL, lgr = NULL, pchdepr = NULL)]
setorder(firms, symbol, date)


# PRERPCESSING ------------------------------------------------------------
# # choose matching predictors
id_cols = c("symbol", "month", "momret")
predictors = colnames(firms)[c(9:ncol(firms))]
predictors = c(predictors, mom_cls)
predictors = setdiff(predictors,
                     c("acceptedDateTime", "month", "date",
                       "industry", "sector","open", "high", "low", "close",
                       "volume", "close_raw", "returns"))
predictors = unique(predictors)

# merge predictors and firms
cols = c(id_cols, predictors)
X = firms[, ..cols]
dim(X)
# X[, date := as.POSIXct(date, tz = "UTC")]

# remove columns with many NA
keep_cols = names(which(colMeans(!is.na(X)) > 0.7)) # DONT DELETE MOM
print(paste0("Removing columns with many NA values: ", setdiff(colnames(X), c(keep_cols, "right_time"))))
X = X[, .SD, .SDcols = keep_cols]

# Remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(X))) > 0.95))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(X), keep_cols)))
X = X[, .SD, .SDcols = keep_cols]

# Number of variables by month
g1 = X[, .N, by = month][order(month)] |>
  ggplot(aes(x = month, y = N)) +
  geom_line() +
  labs(title = "Number of variables by month",
       x = "Month", y = "Number of variables") +
  theme_minimal()
g2 = na.omit(X)[, .N, by = month][order(month)] |>
  ggplot(aes(x = month, y = N)) +
  geom_line() +
  labs(title = "Number of variables by month",
       x = "Month", y = "Number of variables") +
  theme_minimal()
g1 / g2

# remove NA and Inf values
X = na.omit(X)
noninf_cols = which(is.finite(rowSums(X[, 3:ncol(X)])))
X = X[noninf_cols]

# Number of observations
X[, .N, by = month][order(month)] |>
  ggplot(aes(x = month, y = N)) +
  geom_line() +
  labs(title = "Number of variables by month",
       x = "Month", y = "Number of variables") +
  theme_minimal()

# define feature columns
predictors = colnames(X)[colnames(X) %in% predictors]

# remove constant columns
constant_cols = X[, lapply(.SD, function(x) var(x, na.rm=TRUE) == 0), by = month, .SDcols = predictors]
constant_cols = colnames(constant_cols)[constant_cols[, sapply(.SD, function(x) any(x == TRUE))]]
print(paste0("Removing feature with 0 standard deviation: ", constant_cols))
predictors = setdiff(predictors, constant_cols)

# winsorize
X[, (predictors) := lapply(.SD, function(x) {
  Winsorize(x, val = quantile(x, probs = c(0.01, 0.99), na.rm = TRUE))
}), .SDcols = predictors, by = month]

# remove constant columns
constant_cols = X[, lapply(.SD, function(x) var(x, na.rm=TRUE) == 0), by = month, .SDcols = predictors]
constant_cols = colnames(constant_cols)[constant_cols[, sapply(.SD, function(x) any(x == TRUE))]]
print(paste0("Removing feature with 0 standard deviation: ", constant_cols))
predictors = setdiff(predictors, constant_cols)

# remove highly correlated features
cor_mat = mlr3misc::invoke(stats::cor, x = X[, ..predictors])
cor_abs = abs(cor_mat)
cor_abs[upper.tri(cor_abs)] = 0
diag(cor_abs) = 0
to_remove = integer(0)
cutoff_ = 0.98
for (i in seq_len(ncol(cor_abs))) {
  # i = 15
  # print(i)
  # If this column is already marked for removal, skip
  if (i %in% to_remove) next

  # Find columns correlated above the threshold with column i
  high_cor_with_i = which(cor_abs[, i] > cutoff_)

  if (length(high_cor_with_i) != 0) print(i)

  # Mark those columns (except i itself) for removal
  for (j in high_cor_with_i) {
    if (!(j %in% to_remove) && j != i) {
      to_remove = c(to_remove, j)
    }
  }
}
to_remove = unique(to_remove)
keep_idx = setdiff(seq_len(ncol(X[, ..predictors])), to_remove)
predictors = colnames(X[, ..predictors])[keep_idx]

# get final preprocessed X
cols_final = c(id_cols, predictors)
X = X[, ..cols_final]


# PCA ---------------------------------------------------------------------
panel_pca = X[month > 2015, .(symbol = list(symbol),
                  pca = list(prcomp(as.data.frame(.SD), scale. = TRUE))),
              by = month, .SDcols = predictors]

# join rotated data and permno
panel_pca[, pca_predictors := mapply(function(x, y)
  list(cbind.data.frame(symbol = x, y)),
  panel_pca[, symbol], lapply(panel_pca[, pca], `[[`, "x"))
]
panel_pca[1, pca_predictors]

# get cumulative proportions results
get_cumulative_proportion = function(x, var_explained = 0.99) {
  cumulative_proportion <- cumsum(x$sdev^2) / sum(x$sdev^2)
  return(which(cumulative_proportion >= var_explained)[1])
}
panel_pca[, n_over_99 := lapply(pca, function(x) get_cumulative_proportion(x))]
panel_pca[, n_over_95 := lapply(pca, function(x) get_cumulative_proportion(x, 0.95))]
panel_pca[, n_over_90 := lapply(pca, function(x) get_cumulative_proportion(x, 0.90))]

# remove low pca
setorder(panel_pca, month)
panel_pca = panel_pca[n_over_99 > 10]


# # K-MEANS -----------------------------------------------------------------
# parameters
alpha = 50
K = c(5,10,50, 100, 500, 1000,1500)

# select number of components that explains 99% of variance
panel_pca[, pca_99 := mapply(function(x, y) x[, 1:(y+1)],
                             panel_pca[, pca_predictors],
                             panel_pca[, n_over_99])]

# define largest K
f = function(x) K[K < nrow(x)]
panel_pca[, K_max := lapply(pca_99, f)]
panel_pca[, K_max]

# kkmeans estimation function
kkmeans_stimation = function(pca_xx, k = 5) {

  # debug
  # pca_xx = panel_pca[1, pca_99]

  # extract predictors from pca object
  pca_xx = pca_xx[[1]]
  pca_xx = pca_xx[, 2:ncol(pca_xx)]

  # create KMeans model
  km = stats::kmeans(pca_xx,
                     centers = k,
                     iter.max = 1000,
                     nstart = 5,
                     algorithm = "Lloyd")

  # calculate nearest neighbor distance
  dist  = FNN::get.knn(pca_xx, k = 2)

  # get alpha quantile of neigboring distances
  distance_of_percentile_to_nearest_neighbors = median(dist$nn.dist[, 2])

  # get the cluster size
  size = length(km$size)

  # calculate distances to centroide of belonging_cluster for every data point
  centers <- km$centers[km$cluster, ]
  distances <- sqrt(rowSums((pca_xx - centers)^2))

  # outliers
  outliers = distances > distance_of_percentile_to_nearest_neighbors
  # sum(outliers) / length(outliers)

  # merge data, clusters and outliers
  list(cbind.data.frame(cluster = km$cluster, outliers = outliers))
}

# generate kmeans clusters
panel_pca[, kk_5 := list(kkmeans_stimation(pca_99, 5)), by = month]
panel_pca[, kk_10 := list(kkmeans_stimation(pca_99, 10)), by = month]
panel_pca[, kk_50 := list(kkmeans_stimation(pca_99, 50)), by = month]
panel_pca[, kk_500 := list(kkmeans_stimation(pca_99, 500)), by = month]


# CREATE PAIRS ------------------------------------------------------------
# Preparing all data for trading in one table
dt_trading = X[, .(month, symbol, momret, mom1m)]
kmeans_clusters = panel_pca[, cbind(symbol = unlist(symbol), rbindlist(kk_50)), by = month]
dt_trading = merge(dt_trading, kmeans_clusters, by = c("symbol", "month"), all.x = TRUE, all.y = FALSE)
dt_trading = na.omit(dt_trading)
setorder(dt_trading, month)

# remove outliers
dt_trading = dt_trading[outliers == FALSE]

# order from largest to lowst mom1m
setorderv(dt_trading, c("month", "cluster", "mom1m"), order = -1)

# calclualte mom1m diference between all pairs
dt_trading[, mom1m_diff := unlist(lapply(0:(length(mom1m) - 1), function(x)
  mom1m[x + 1] - mom1m[length(mom1m) - x])),
  by = c("month", "cluster")]
dt_trading[, mom1m_diff_sd := sd(mom1m_diff), by = c("month")] # by = c("date", "cluster")

# filter pairs above threshold
universe = dt_trading[abs(mom1m_diff) > mom1m_diff_sd]
setorder(universe, month, cluster, -mom1m)
universe[, .N, by = c("month", "cluster")]

# create pairs data table
pairs = universe[, `:=`(pair_short = symbol,
                        pair_long = rev(symbol),
                        momret_short = momret,
                        momret_long = rev(momret)), by = c("month", "cluster")]
pairs = pairs[mom1m_diff > 0]
pairs[month == 2025 & cluster == 8,
      .(month, symbol, pair_short, pair_long, momret_short, momret_long)]

# inspect pairs
pairs[, .N, c("month", "cluster")]

# cumulative returns by date
returns_long = pairs[, .(cum_return = sum(momret_long * (1 / length(momret_long)))), by = "month"]
returns_short = pairs[, .(cum_return = sum(-momret_short * (1 / length(momret_short)))), by = "month"]

# performance by dates
returns_long[, mean(cum_return), by = "month"]
returns_short[, mean(cum_return), by = "month"]
returns_long[, median(cum_return), by = "month"]
returns_short[, median(cum_return), by = "month"]

# Equity curve
portfolio = as.xts.data.table(returns_long[, .(zoo::as.Date.yearmon(month), cum_return)])
charts.PerformanceSummary(portfolio, main = "Long Pairs")
charts.PerformanceSummary(portfolio["2020/"], main = "Long Pairs")


# SAVE FOR QC BACKTEST ----------------------------------------------------
# extract dtaa for best clustering method
pairs_qc = pairs[, .(month, pair_short, pair_long)]
pairs_qc = pairs_qc[, .(pairs_short = paste0(pair_short, collapse = "|"),
                        pairs_long = paste0(pair_long, collapse = "|")),
                    by = month]
pairs_qc[, month := as.Date(zoo::as.yearmon(month))]
pairs_qc[, month := ceiling_date(month, "month") - 1]
setorder(pairs_qc, "month")
# seq_date = data.table(month = seq.Date(min(pairs_qc$month), max(pairs_qc$year_month_id), by = 1))
# pairs_qc = pairs_qc[seq_date, on = "month", roll = Inf]
pairs_qc[, year_month_id := as.character(month)]
pairs_qc = pairs_qc[month >= as.Date("2020-01-01")]
pairs_qc[, month := as.Date(vapply(month, advanceDate, FUN.VALUE = Date(1L)))]
endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"),
                            key=Sys.getenv("BLOB-KEY"))
cont = storage_container(endpoint, "qc-backtest")
storage_write_csv(pairs_qc, cont, "fundamental_stat_arb.csv", col_names = FALSE)

# Compare QC and local
pairs_qc[, unique(month)]
head(pairs_qc)



# # PREDICTORS VLADO --------------------------------------------------------
# # import data set
# df = fread("C:/Users/Mislav/Documents/GitHub/Pairs-Trading-via-Unsupervised-Learning/datashare.csv")
#
# # filter dates
# df_radni = df[between(DATE, 19751231, 20220101, incbounds = FALSE)]
#
# # choose columns
# cols = c(
#   'DATE','permno','mom1m','sic2','absacc','acc','aeavol','age','agr','baspread',
#   'beta','betasq','bm','bm_ia','cash','cashdebt','cashpr','cfp','cfp_ia',
#   'chatoia','chcsho','chempia','chinv','chmom','chpmia','chtx','cinvest',
#   'convind','currat','depr','divi','divo','dolvol','dy','ear','egr','ep',
#   'gma','herf','hire','idiovol','ill','indmom','invest','lev','lgr','maxret',
#   'ms','mve_ia','mvel1','nincr','operprof','pchcapx_ia','pchcurrat','pchdepr',
#   'pchgm_pchsale','pchquick','pchsale_pchrect','pctacc','pricedelay',
#   'ps','quick','rd','retvol','roaq','roeq','roic','rsup','salecash',
#   'salerec','securedind','sgr','sin','sp','std_dolvol','std_turn',
#   'tang','tb','turn','zerotrade'
# )
# df_radni = df_radni[, ..cols] # Vlado (3579135, 80) -> SAME
#
# # feture names
# feature_names_list = c('mom1m','absacc','acc','aeavol','age','agr','baspread',
#                        'beta','betasq','bm','bm_ia','cash','cashdebt','cashpr','cfp','cfp_ia',
#                        'chatoia','chcsho','chempia','chinv','chmom','chpmia','chtx','cinvest',
#                        'convind','currat','depr','divi','divo','dolvol','dy','ear','egr','ep',
#                        'gma','herf','hire','idiovol','ill','indmom','invest','lev','lgr','maxret',
#                        'ms','mve_ia','mvel1','nincr','operprof','pchcapx_ia','pchcurrat','pchdepr',
#                        'pchgm_pchsale','pchquick','pchsale_pchrect','pctacc','pricedelay',
#                        'ps','quick','rd','retvol','roaq','roeq','roic','rsup','salecash',
#                        'salerec','securedind','sgr','sin','sp','std_dolvol','std_turn',
#                        'tang','tb','turn','zerotrade')
# mom_names_list = c("mom1m","mom2m","mom3m","mom4m","mom5m","mom6m","mom7m","mom8m","mom9m","mom10m","mom11m","mom12m",
#                    "mom13m","mom14m","mom15m","mom16m","mom17m","mom18m","mom19m","mom20m","mom21m","mom22m","mom23m","mom24m",
#                    "mom25m","mom26m","mom27m","mom28m","mom29m","mom30m","mom31m","mom32m","mom33m","mom34m","mom35m","mom36m",
#                    "mom37m","mom38m","mom39m","mom40m","mom41m","mom42m","mom43m","mom44m","mom45m","mom46m","mom47m","mom48m")
# feature_names_list_full = c(feature_names_list, mom_names_list)
#
#
#
# ######### RIGHT WAY ###########
# # parameters
# max_number_of_nulls_in_observation = 2
#
# # remove nas by n NA's in rows
# delete_na <- function(DT, n=0) {
#   DT[rowSums(is.na(DT)) <= n]
# }
#
# # calucalte mom features and remove features with more than 2 NA's in features
# start_time = Sys.time()
# panel = as.data.table(expand.grid(permno = df_radni[, unique(permno)],
#                                   DATE = df_radni[, unique(DATE)]))
# panel = df_radni[panel, on = c("permno", "DATE")]
# mom_size = 2:48
# mom_cols = paste0("mom", mom_size, "m")
# setorder(panel, permno, DATE)
# panel[permno == 10006, .(DATE, mom1m)][1:100]
# duplicated(panel[permno == 10006, .(DATE)])
# setorder(panel, permno, DATE)
# panel[, (mom_cols) := frollapply(shift(mom1m, 1), n = mom_size-1,
#                                  function(y) prod(1+y)-1),
#       by = permno]
# end_time = Sys.time()
# end_time - start_time
#
# # check permno na.permno == 10006
# panel_2 = na.omit(panel, cols = c("mom1m", mom_cols))
# panel_2 = panel_2[rowSums(is.na(panel_2[, ..feature_names_list])) <= 2]
# nrow(panel_2)
#
# # impute missing values with median
# cols = c(feature_names_list, mom_names_list)
# panel_2[, (cols) := lapply(.SD, function(x){
#   fifelse(is.na(x), median(x, na.rm = TRUE), x)
# }), .SDcols = cols, by = "DATE"]
# setorderv(panel_2, c("permno", "DATE"))
#
# # compare
# dim(panel_2)
#
# # check for constant columns for every date
# constant_columns = panel_2[, lapply(.SD, function(x) var(x) == 0), by = DATE]
# constant_columns_subset <- constant_columns[, .SD,
#                                             .SDcols = which(sapply(constant_columns, function(x) any(x == TRUE)))]
#
# # temporarily solution - remove salerec column because it is constant across some dates
# panel_2[, colnames(constant_columns_subset) := NULL]
# festures_remove = c("sic2", colnames(constant_columns_subset))
# feature_names_list_full = setdiff(feature_names_list_full, festures_remove)
#
# # aplly PCA
# panel_pca = panel_2[, .(permno = list(permno),
#                         pca = list(prcomp(as.data.frame(.SD), scale. = TRUE))),
#                     by = DATE, .SDcols = feature_names_list_full]
#
# # join rotated data and permno
# # test = mapply(function(x, y) list(cbind(permno = x, y)), panel_pca[1:2, permno], lapply(panel_pca[1:2, pca], `[[`, "x"))
# # test = copy(panel_pca[1:2])
# # test[, pca_predictors := mapply(function(x, y) list(cbind(permno = x, y)),
# #                                       panel_pca[1:2, permno], lapply(panel_pca[1:2, pca], `[[`, "x"))]
# panel_pca[, pca_predictors := mapply(function(x, y) list(cbind(permno = x, y)),
#                                      panel_pca[, permno], lapply(panel_pca[, pca], `[[`, "x"))]
# panel_pca[1, pca_predictors]
#
# # get cumulative proportions results
# # panel_pca[1:2, lapply(pca, function(x) get_cumulative_proportion(x))]
# get_cumulative_proportion = function(x, var_explained = 0.99) {
#   cumulative_proportion <- cumsum(x$sdev^2 / sum(x$sdev^2))
#   return(which(cumulative_proportion >= var_explained)[1])
# }
# panel_pca[, n_over_99 := lapply(pca, function(x) get_cumulative_proportion(x))]
# panel_pca[, n_over_95 := lapply(pca, function(x) get_cumulative_proportion(x, 0.95))]
# panel_pca[, n_over_90 := lapply(pca, function(x) get_cumulative_proportion(x, 0.90))]
#
#
#
# # K-MEANS -----------------------------------------------------------------
# # parameters
# alpha = 50
# K = c(5,10,50, 100, 500, 1000,1500)
#
# # select number of components that explains 99% of variance
# panel_pca[, pca_99 := mapply(function(x, y) x[, 1:(y+1)],
#                              panel_pca[, pca_predictors],
#                              panel_pca[, n_over_99])]
#
# # define largest K
# f = function(x) K[K < nrow(x)]
# panel_pca[, K_max := lapply(pca_99, f)]
# panel_pca[, K_max]
#
# # kkmeans estimation function
# kkmeans_stimation = function(pca_xx, k = 5) {
#
#   # debug
#   # pca_xx = panel_pca[1, pca_99]
#
#   # extract predictors from pca object
#   pca_xx = pca_xx[[1]]
#   pca_xx = pca_xx[, 2:ncol(pca_xx)]
#
#   # create KMeans model
#   km = stats::kmeans(pca_xx,
#                      centers = k,
#                      iter.max = 1000,
#                      nstart = 5,
#                      algorithm = "Lloyd")
#
#   # calculate nearest neighbor distance
#   dist  = FNN::get.knn(pca_xx, k = 2)
#
#   # get alpha quantile of neigboring distances
#   distance_of_percentile_to_nearest_neighbors = median(dist$nn.dist[, 2])
#
#   # get the cluster size
#   size = length(km$size)
#
#   # calculate distances to centroide of belonging_cluster for every data point
#   centers <- km$centers[km$cluster, ]
#   distances <- sqrt(rowSums((pca_xx - centers)^2))
#
#   # outliers
#   outliers = distances > distance_of_percentile_to_nearest_neighbors
#   # sum(outliers) / length(outliers)
#
#   # merge data, clusters and outliers
#   list(cbind.data.frame(cluster = km$cluster, outliers = outliers))
# }
#
# # generate kmeans clusters
# # panel_pca[, kk_5 := list(kkmeans_stimation(pca_99, 5)), by = DATE]
# # panel_pca[, kk_10 := list(kkmeans_stimation(pca_99, 10)), by = DATE]
# # panel_pca[, kk_50 := list(kkmeans_stimation(pca_99, 50)), by = DATE]
# # panel_pca[, kk_100 := list(kkmeans_stimation(pca_99, 100)), by = DATE]
# panel_pca[, kk_500 := list(kkmeans_stimation(pca_99, 500)), by = DATE]
#
#
#
# # TRADING -----------------------------------------------------------------
# # Preparing all data for trading in one table
# df_data_for_trading = panel[, .(DATE, permno, mom1m)]
# setnames(df_data_for_trading, "mom1m", "rt-1")
# setorder(df_data_for_trading, permno, DATE)
# df_data_for_trading[, rt := shift(`rt-1`, type = "lead"), by = .(permno)]
# kmeans_clusters = panel_pca[, cbind(permno = unlist(permno), rbindlist(kk_500)), by = DATE]
# df_data_for_trading = merge(df_data_for_trading, kmeans_clusters, by = c("permno", "DATE"), all.x = TRUE, all.y = FALSE)
# df_data_for_trading = na.omit(df_data_for_trading)
# df_data_for_trading = df_data_for_trading[DATE %between% c(19800131, 20201231)]
# setorder(df_data_for_trading, DATE)
#
#
# # date window
# date = 19800131
# df_data_for_trading_window = df_data_for_trading[DATE == date]
# KMeans_unique_clusters_for_date = df_data_for_trading_window[, unique(cluster)]
# table(df_data_for_trading_window[, cluster])
# df_data_for_trading_window[, .N, by = cluster][order(N)]



# DAILY MARKET DATA -------------------------------------------------------
# # import market data (choose frequency)
# arr <- tiledb_array("D:/equity-usa-daily-fmp",
#                     as.data.frame = TRUE,
#                     query_layout = "UNORDERED",
#                     selected_ranges = list(symbol = cbind(sp500_symbols, sp500_symbols))
# )
# system.time(prices <- arr[])
# tiledb_array_close(arr)
# prices <- as.data.table(prices)
#
# # remove duplicates
# prices_dt <- unique(prices, by = c("symbol", "date"))
#
# # change date to data.table date
# prices_dt[, date := data.table::as.IDate(date)]

# # keep only NYSE trading days
# trading_days <- getBusinessDays(prices_dt[, min(date)], prices_dt[, max(date)])
# setkey(prices_dt, date)
# prices_dt <- prices_dt[.(as.IDate(trading_days))]
# setkey(prices_dt, NULL)
#
# # remove observations with measurement errors
# prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
#
# # order data
# setorder(prices_dt, "symbol", "date")
#
# # adjuset all prices, not just close
# prices_dt[, returns := adjClose / shift(adjClose) - 1, by = symbol] # calculate returns
# prices_dt <- prices_dt[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%
# adjust_cols <- c("open", "high", "low")
# prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
# prices_dt[, close := adjClose]
#
# # remove missing values
# prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume, returns)])
#
# # remove symobls with < 252 observations
# prices_n <- prices_dt[, .N, by = symbol]
# prices_n <- prices_n[N > 252]  # remove prices with only 700 or less observations
# prices_dt <- prices_dt[symbol %in% prices_n[, symbol]]
#
#
# # save SPY for later and keep only events symbols
# spy <- prices_dt[symbol == "SPY"]
# setorder(spy, date)



# # FUNDAMENTAL DATA --------------------------------------------------------
# # tiledb urls
# uri_pl = "s3://equity-usa-income-statement-bulk"
# uri_bs = "s3://equity-usa-balance-sheet-statement-bulk"
# uri_fg = "s3://equity-usa-financial-growth-bulk"
# uri_metrics = "s3://equity-usa-key-metrics-bulk"
#
# # income statement data
# arr <- tiledb_array(uri_pl, as.data.frame = TRUE, query_layout = "UNORDERED",)
# system.time(pl <- arr[])
# tiledb_array_close(arr)
# pl <- as.data.table(pl)
# pl[, `:=`(acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"),
#           acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"))]
#
# # balance sheet data
# arr <- tiledb_array(uri_bs, as.data.frame = TRUE, query_layout = "UNORDERED")
# system.time(bs <- arr[])
# tiledb_array_close(arr)
# bs <- as.data.table(bs)
# bs[, `:=`(acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"),
#           acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"))]
#
# # financial growth
# arr <- tiledb_array(uri_fg, as.data.frame = TRUE, query_layout = "UNORDERED")
# system.time(fin_growth <- arr[])
# tiledb_array_close(arr)
# fin_growth <- as.data.table(fin_growth)
#
# # financial ratios
# arr <- tiledb_array(uri_metrics, as.data.frame = TRUE, query_layout = "UNORDERED")
# system.time(fin_ratios <- arr[])
# tiledb_array_close(arr)
# fin_ratios <- as.data.table(fin_ratios)
#
# # merge all fundamental data
# columns_diff_pl <- c("symbol", "date", setdiff(colnames(pl), colnames(bs)))
# columns_diff_fg <- c("symbol", "date", setdiff(colnames(fin_growth), colnames(pl)))
# columns_diff_fr <- c("symbol", "date", setdiff(colnames(fin_ratios), colnames(pl)))
# fundamentals <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
#                        list(bs,
#                             pl[, ..columns_diff_pl],
#                             fin_growth[, ..columns_diff_fg],
#                             fin_ratios[, ..columns_diff_fr]))



# X -----------------------------------------------------------------------
# choose matching predictors
id_cols = c("symbol", "month", "momret")
predictors = colnames(firms)[108:ncol(firms)]
predictors = c(predictors, mom_cls)
predictors = setdiff(predictors,
                     c("acceptedDateTime", "month", "date",
                       "industry", "sector","open", "high", "low", "close",
                       "volume", "close_raw", "returns"))
predictors = unique(predictors)

# merge predictors and firms
cols = c(id_cols, predictors)
X = firms[, ..cols]
dim(X)

# remove columns with many NA
keep_cols <- names(which(colMeans(!is.na(X)) > 0.80))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(X), c(keep_cols, "right_time"))))
X <- X[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(X))) > 0.99))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(X), keep_cols)))
X = X[, .SD, .SDcols = keep_cols]

# prepare data for clustering
X = na.omit(X)

# define feature columns
predictors = colnames(X)[colnames(X) %in% predictors]

# Winsorize
X[, (predictors) := lapply(.SD, function(x) {
  Winsorize(x, val = quantile(x, probs = c(0.01, 0.99), na.rm = TRUE))
}), .SDcols = predictors, by = month]

# remove constant columns
constant_cols = X[, lapply(.SD, function(x) var(x, na.rm=TRUE) == 0), by = month, .SDcols = predictors]
constant_cols = colnames(constant_cols)[constant_cols[, sapply(.SD, function(x) any(x == TRUE))]]
print(paste0("Removing feature with 0 standard deviation: ", constant_cols))
predictors = setdiff(predictors, constant_cols)

# remove highly correlated features
cor_mat = mlr3misc::invoke(stats::cor, x = X[, ..predictors])
cor_abs = abs(cor_mat)
cor_abs[upper.tri(cor_abs)] = 0
diag(cor_abs) = 0
to_remove = integer(0)
cutoff_ = 0.98
for (i in seq_len(ncol(cor_abs))) {
  # i = 15
  # print(i)
  # If this column is already marked for removal, skip
  if (i %in% to_remove) next

  # Find columns correlated above the threshold with column i
  high_cor_with_i = which(cor_abs[, i] > cutoff_)

  if (length(high_cor_with_i) != 0) print(i)

  # Mark those columns (except i itself) for removal
  for (j in high_cor_with_i) {
    if (!(j %in% to_remove) && j != i) {
      to_remove = c(to_remove, j)
    }
  }
}
to_remove = unique(to_remove)
keep_idx = setdiff(seq_len(ncol(X[, ..predictors])), to_remove)
predictors = colnames(X[, ..predictors])[keep_idx]

# get final preprocessed X
cols_final = c(id_cols, predictors)
X = X[, ..cols_final]

# Scale X
# X = X[, (predictors) := lapply(.SD, scale), .SDcols = predictors]

# PCA that explains 99% of variance
pca <- prcomp(as.data.frame(X[, ..predictors]), scale = TRUE, center = TRUE)
cumulative_proportion <- cumsum(pca$sdev^2 / sum(pca$sdev^2))
n_components <- which(cumulative_proportion >= 0.99)[1]
X_pca <- prcomp(as.data.frame(X[, ..feature_cols]), scale = FALSE, rank = n_components)
cols_keep = c(id_cols, "mom1m")
X_pca = cbind(X[, ..cols_keep], X_pca$x)

# PCA ---------------------------------------------------------------------
# REshape data
panel_pca = X[month > 2015, .(symbol = list(symbol),
                              pca = list(prcomp(as.data.frame(.SD), center = TRUE, scale. = TRUE))),
              by = month, .SDcols = predictors]

# join rotated data and permno
panel_pca[, pca_predictors := mapply(function(x, y)
  list(cbind.data.frame(symbol = x, y)),
  panel_pca[, symbol], lapply(panel_pca[, pca], `[[`, "x"))
]
panel_pca[1, pca_predictors]

# get cumulative proportions results
get_cumulative_proportion = function(x, var_explained = 0.99) {
  cumulative_proportion <- cumsum(x$sdev^2) / sum(x$sdev^2)
  return(which(cumulative_proportion >= var_explained)[1])
}
panel_pca[, n_over_99 := lapply(pca, function(x) get_cumulative_proportion(x))]
panel_pca[, n_over_95 := lapply(pca, function(x) get_cumulative_proportion(x, 0.95))]
panel_pca[, n_over_90 := lapply(pca, function(x) get_cumulative_proportion(x, 0.90))]

# remove low pca
setorder(panel_pca, month)
panel_pca = panel_pca[n_over_99 > 10]


# MLR3 ESTIMATION ---------------------------------------------------------
# create tasks for every month
tasks = lapply(dates, function(d) {
  task = as_task_clust(X_pca[as.IDate(date) == as.IDate(d)],
                       id = paste0("Date ", d))
  task$col_roles$feature = setdiff(task$col_roles$feature, cols_keep)
  return(task)
})

# benchmark with various tasks
design = benchmark_grid(
  tasks = tasks,
  learners = list(
    lrn("clust.kmeans", centers = 5L, id = "kmeans_5"),
    lrn("clust.kmeans", centers = 10L, id = "kmeans_10"),
    lrn("clust.kmeans", centers = 50L, id = "kmeans_50")),
  # lrn("clust.pam", k = 3L),
  # lrn("clust.hclust"),
  # lrn("clust.diana", k = 5L, id = "diana_5"),
  # lrn("clust.diana", k = 10L, id = "diana_10"),
  # lrn("clust.diana", k = 50L, id = "diana_50"),
  #
  # lrn("clust.agnes", k = 5L, id = "agnes_5"),
  # lrn("clust.agnes", k = 10, id = "agnes_10"),
  # lrn("clust.agnes", k = 50L, id = "agnes_50"),
  # lrn("clust.cmeans", centers = 5L, id = "cmeans_5"),
  # lrn("clust.cmeans", centers = 10L, id = "cmeans_10"),
  # lrn("clust.cmeans", centers = 50L, id = "cmeans_50")),
  resamplings = rsmp("insample"))
bmrs = benchmark(design, store_models = TRUE)
bmr_dt = as.data.table(bmrs)

# get predictions for all tasks and learners
tasks = bmr_dt$task
task_names = vapply(tasks, function(x) x$id, FUN.VALUE = character(1))
backs = lapply(tasks, function(x) {
  x$backend$data(cols = c(cols_keep, x$feature_names),
                 rows = 1:x$nrow)
})
names(backs) = task_names
backs_dt = rbindlist(backs, idcol = "task_id")
learners = bmr_dt$learner
learner_names = vapply(learners, function(x) x$id, FUN.VALUE = character(1))
predictions = bmr_dt$prediction
names(predictions) = learner_names
predictions_dt = lapply(predictions, as.data.table)
predictions_dt = rbindlist(predictions_dt, idcol = "learner_id", fill = TRUE)
predictions_dt = predictions_dt[, 1:3]

# help col names
cols_by = c("task_id", "learner_id")

# merge predictions and backends
clusters = cbind(backs_dt, predictions_dt)
setorderv(clusters, c("task_id", "learner_id", "partition", "mom1m"), order = -1)

# calclualte mom1m diference between all pairs
clusters[, mom1m_diff := unlist(lapply(0:(length(mom1m) - 1), function(x) mom1m[x+1] - mom1m[length(mom1m)-x])),
         by = c(cols_by, "partition")]
clusters[, mom1m_diff_sd := sd(mom1m_diff), by = cols_by]



# INDIVIDUAL ESTIMATION - AGGLOMERATIVE SLUSTERING ----------------------------
# prepare data
dt = copy(X_pca)
predictors = setdiff(colnames(dt), c(id_cols, "mom1m"))

# Specify alpha percentile for linkage distance
alpha <- 0.1

# Calculate l1 distance between nearest data points
dist_matrix <- dist(dt[, ..predictors], method = "manhattan") # Calculate distance matrix
min_dist <- min(dist(dist_matrix, method = "manhattan"))

# Calculate linkage distance as alpha percentile of nearest data points distance
linkage_distance <- alpha * min_dist

# Perform agglomerative clustering with average linkage and specified linkage distance
hclust_result <- hclust(dist_matrix, method = "average") # Perform hierarchical clustering with average linkage
cut_dendrogram <- cutree(hclust_result, h = linkage_distance) # Cut dendrogram at specified linkage distance




# REMOVE OUTLIERS ---------------------------------------------------------
# identify outliers for kmeans clusters
feature_names = tasks[[1]]$feature_names
models = lapply(learners, `[[`, "model")
models_dt = as.data.table(cbind.data.frame(task_names, learner_names))
models_dt[, models := list(models)]

# debug for kmeans
data_ = tasks[[1]]$data()
kmeans_model = bmr_dt[1]$learner[[1]]$model

# util function to find nearest neighbour distance
get_threshold_e = function(df, alpha = 0.5) {
  nn_distances =  as.matrix(dist(df))
  nn_distances = apply(nn_distances, 1, sort)
  nn_distances = apply(nn_distances, 1, `[`, 2)
  e = quantile(nn_distances, probs = alpha)
  return(e)
}

# nearest neighbor distances
e_thresholds_by_ids = clusters[, .(l2 = get_threshold_e(.SD)), .SDcols = feature_names, by = cols_by]
e_thresholds_by_ids_kmeans = e_thresholds_by_ids[grep("kmeans", learner_id)]

# calculate distance to centroid for kmeans clusters
kmeans_index = grep("kmeans", vapply(bmr_dt[, learner], `[[`, "id", FUN.VALUE = character(1L)))
centers = lapply(bmr_dt[, learner], function(x) x$model$centers)[kmeans_index]
data_ = lapply(bmr_dt[, task], function(x) x$data())[kmeans_index]
partritions = lapply(bmr_dt[, learner], function(x) length(x$model$size))[kmeans_index]
centroid_distances = lapply(1:length(data_), function(i) {
  x = head(as.matrix(dist(rbind(data_[[i]], centers[[i]]))), -partritions[[i]])
  x = x[, ((ncol(x)-partritions[[i]]+1):ncol(x))]
  as.data.table(x)
  # return(as.vector(x))
})
centroid_distances[[1]]
centroid_distances[[2]]
outliers = lapply(seq_along(centroid_distances), function(i) {
  apply(centroid_distances[[i]], 1, function(x) all(x > e_thresholds_by_ids_kmeans[i, l2]))
})

# add outliers to clusters object
clusters[grep("kmeans", learner_id), outlier := unlist(outliers)]



# CREATE PAIRS ------------------------------------------------------------
# filter pairs above threshold
universe = clusters[abs(mom1m_diff) > mom1m_diff_sd]
universe[, .N, by = c(cols_by, "partition")]

# create pairs data table
pairs = universe[, `:=`(pair_short = symbol,
                        pair_long = rev(symbol),
                        momret_short = momret,
                        momret_long = rev(momret)), by = c(cols_by, "partition")]
pairs[task_id == "Date 2023-01-01" & partition == 3 & learner_id == "kmeans_50",
      .(date, symbol, pair_short, pair_long, momret_short, momret_long)]
pairs = pairs[mom1m_diff > 0]

# remove outliers
dim(pairs)
pairs = pairs[outlier == FALSE]
dim(pairs)

# inspect pairs
pairs[, .N, c(cols_by, "partition")]

# cumulative returns ny tasks and partitions
returns_long = pairs[, .(cum_return = prod(1+momret_long)-1), by = cols_by]
returns_short = pairs[, .(cum_return = prod(1+(-momret_short)-1)), by = cols_by]

# performance by dates
returns_long[, mean(cum_return), by = "task_id"]
returns_short[, mean(cum_return), by = "task_id"]
returns_long[, median(cum_return), by = "task_id"]
returns_short[, median(cum_return), by = "task_id"]

# perfromance by learners
returns_long[, mean(cum_return), by = "learner_id"]
returns_short[, mean(cum_return), by = "learner_id"]
returns_long[, median(cum_return), by = "learner_id"]
returns_short[, median(cum_return), by = "learner_id"]



# SAVE FOR QC BACKTEST ----------------------------------------------------
# extract dtaa for best clustering method
pairs_qc = pairs[learner_id == "kmeans_50"]
pairs_qc = pairs_qc[, .(date, pair_short, pair_long)]
pairs_qc = pairs_qc[, .(pairs_short = paste0(pair_short, collapse = "|"),
                        pairs_long = paste0(pair_long, collapse = "|")),
                    by = date]
pairs_qc[, date := as.Date(date)]
setorder(pairs_qc, "date")
seq_date = data.table(date = seq.Date(min(pairs_qc$date), max(pairs_qc$date), by = 1))
pairs_qc = pairs_qc[seq_date, on = "date", roll = Inf]
pairs_qc[, date := as.character(date)]
endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"),
                            key=Sys.getenv("BLOB-KEY"))
cont = storage_container(endpoint, "qc-backtest")
storage_write_csv(pairs_qc, cont, "fundamental_stat_arb.csv", col_names = FALSE)
