library(fastverse)
library(finutils)
library(ggplot2)
library(PerformanceAnalytics)
library(lubridate)


# Import prices
prices = qc_hour(
  file_path = "F:/lean/data/stocks_hour.csv",
  first_date = as.Date("2015-01-01"),
  min_obs = 7*44,
  duplicates = "fast",
  add_dv_rank = FALSE
)
prices[, time := as.ITime(date)]

# Keep hour 09:00, 15:00
dt = prices[time %in% c(as.ITime("09:00:00"), as.ITime("15:00:00")),
            .(symbol, date, open, close, volume, close_raw, time)]
dt[time == as.ITime("09:00:00"), first_price := close]
dt[time == as.ITime("15:00:00"), last_price := close]
dt[, day := as.Date(date)]

# Create signal
back = dcast(dt[, .(symbol, date = as.Date(date), time, close, open, close_raw)],
             symbol + date ~ time, value.var = list("close", "open", "close_raw"))
back = back[, .(symbol, date, p1 = `close_09:00:00`, p2 = `open_15:00:00`, p1raw = `close_raw_15:00:00`)]

# # Trading logic 1
# back[, target_1 := shift(p1, 1, type = "lead") / p2 - 1, by = symbol]
# back[, target_2 := shift(p2, 1, type = "lead") / p2 - 1, by = symbol]
# back[, target_3 := shift(p2, 2, type = "lead") / p2 - 1, by = symbol]
# back[, target_4 := shift(p2, 3, type = "lead") / p2 - 1, by = symbol]
# back = na.omit(back)
# levels = c(1)
# back[, signal := fcase(
#   (p1 / levels[1]) < 0.98 & (p2 / levels[1]) > 1.02, 1,
#   # p1 < levels[2] & p2 > -levels[2], -1,
#   # p1 < levels[3] & p2 > -levels[3], 1,
#   # p1 < levels[4] & p2 > -levels[4], -1,
#   # p1 < levels[5] & p2 > -levels[5], 1,
#   default = 0
# )]
# back[signal == 1]
# back[signal == 1, .(target_1 = mean(target_1),
#                     target_2 = mean(target_2),
#                     target_3 = mean(target_3),
#                     target_4 = mean(target_4))]
# portfolio = back[signal == 1]
# portfolio[, weight := 0.02]
# portfolio_xts = portfolio[, .(ret = sum(weight * target_1, na.rm = TRUE)), by = date]
# setorder(portfolio_xts, date)
# portfolio_xts = as.xts.data.table(portfolio_xts)
# charts.PerformanceSummary(portfolio_xts)

# Trading logic 2
back[, week_year := ceiling_date(date, "week")]
# back[, week_year := ceiling_date(date, "month")]
backw = back[, .(
  p1 = data.table::first(p1),
  p2 = data.table::last(p2),
  p1raw = data.table::first(p1raw)),
  by = .(symbol, week_year)]
backw[, target_1 := p2 / p1 - 1]
backw[, target_1 := shift(target_1, 1, type = "lead"), by = symbol]
backw = na.omit(backw)
backw = backw[p1raw %between% c(0.05, 10)]
levels = c(1)
backw[, signal := fcase(
  p1 < levels[1] & p2 > levels[1], 1,
  # (p1 / levels[2]) < 0.99 & (p2 / levels[2]) > 1.01, 1,
  # (p1 / levels[3]) < 0.99 & (p2 / levels[3]) > 1.01, 1,
  default = 0
)]
backw[signal == 1]
backw[signal == 1, .(target_1 = mean(target_1))]
portfolio = backw[signal == 1]
portfolio[, weight := 1 / length(p1), by = week_year]
# portfolio[, weight := min(1 / length(p1), 0.03), by = week_year]
# portfolio[, sum(weight), by = week_year][order(week_year)]
portfolio_xts = portfolio[, .(ret = sum(weight * target_1, na.rm = TRUE)), by = week_year]
setorder(portfolio_xts, week_year)
portfolio_xts = as.xts.data.table(portfolio_xts)
charts.PerformanceSummary(portfolio_xts)
SharpeRatio.annualized(portfolio_xts, scale = 52, Rf = 0.0)
Return.annualized(portfolio_xts, scale = 52)

# Trading logic 3
back[, week_year := ceiling_date(date, "week")]
backw = back[, .(
  p1 = data.table::first(p1),
  p2 = data.table::last(p2),
  p1raw = data.table::first(p1raw)),
  by = .(symbol, week_year)]
backw[, target_1 := p2 / p1 - 1]
backw[, target_1 := shift(target_1, 1, type = "lead"), by = symbol]
backw = na.omit(backw)
levels = c(0.4)
backw[, signal := fcase(
  p1 < levels[1] & p2 > levels[1], 1,
  default = NA
)]
backw[signal == 1]
backw[, signal := nafill(signal, type = "locf"), by = symbol]
backw[, signal := ifelse(signal == 1 & (p1 < 5 & p1 > 0.1), 1, NA)]
backw[signal == 1, .(target_1 = mean(target_1))]
portfolio = backw[signal == 1]
portfolio[, weight := 1 / length(p1), by = week_year]
portfolio_xts = portfolio[, .(ret = sum(weight * target_1, na.rm = TRUE)), by = week_year]
setorder(portfolio_xts, week_year)
portfolio_xts = as.xts.data.table(portfolio_xts)
charts.PerformanceSummary(portfolio_xts)
SharpeRatio.annualized(portfolio_xts, scale = 52, Rf = 0.0)
Return.annualized(portfolio_xts, scale = 52)
min(Drawdowns(portfolio_xts))

# backtst function
backtest = function(back, level) {
  back[, signal := fcase(p1 < level & p2 > level, 1,default = NA)]
  back[, signal := nafill(signal, type = "locf"), by = symbol]
  portfolio = back[signal == 1]
  portfolio[, weight := 1 / length(p1), by = week_year]
  portfolio_xts = portfolio[, .(ret = sum(weight * target_1, na.rm = TRUE)), by = week_year]
  setorder(portfolio_xts, week_year)
  portfolio_xts = as.xts.data.table(portfolio_xts)
  cbind.data.frame(
    reta = Return.annualized(portfolio_xts, scale = 52)[1, 1],
    sharpe = SharpeRatio.annualized(portfolio_xts, scale = 52, Rf = 0.0)[1, 1],
    dd = min(Drawdowns(portfolio_xts))
  )
}
grid = c(
  seq(0.1, 10, by = 0.1),
  10:100
)
res_l = lapply(grid, function(x) backtest(copy(backw), x))
res = rbindlist(res_l)
res[, grid := grid]
head(res, 60)
ggplot(res, aes(x = grid, y = reta)) +
  geom_line() +
  geom_point() +
  labs(title = "Return vs Level", x = "Level", y = "Return") +
  theme_minimal()
ggplot(res, aes(x = grid, y = sharpe)) +
  geom_line() +
  geom_point() +
  labs(title = "Return vs Level", x = "Level", y = "Return") +
  theme_minimal()
ggplot(res, aes(x = grid, y = dd)) +
  geom_line() +
  geom_point() +
  labs(title = "Return vs Level", x = "Level", y = "Return") +
  theme_minimal()


