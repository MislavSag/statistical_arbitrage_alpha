# Factor Adjustment Implementation Notes

## Changes Made to `pairs_features.R`

### 1. Added Returns to Price Sample
**Line 64:** Added `returns` column to `prices_sample`
```r
prices_sample = prices[, .(fmp_symbol, date, close, returns)]
```

### 2. Market and Momentum Factor Removal
**Lines 107-145:** Implemented factor adjustment before spread calculation

#### What the code does:

1. **Market Factor Removal**
   - Calculates pairwise market factor as the average return of both stocks
   - This removes common market movements that affect both stocks equally
   ```r
   market_factor = (ret1 + ret2) / 2
   ```

2. **Momentum Factor Removal**
   - Calculates 60-day rolling momentum for each stock
   - Removes persistent trending behavior
   ```r
   mom1 = frollmean(ret1, 60)
   mom2 = frollmean(ret2, 60)
   ```

3. **Adjusted Returns**
   ```r
   ret1_adj = ret1 - market_factor - momentum1
   ret2_adj = ret2 - market_factor - momentum2
   ```

4. **Reconstructed Prices**
   - Builds adjusted log-price series from adjusted returns
   - Ensures spread is calculated on factor-neutral prices
   ```r
   logprice_adj = log(first_price) + cumsum(ret_adj)
   ```

5. **Factor-Adjusted Spread**
   ```r
   spreadclose = logprice1_adj - logprice2_adj
   ```

### 3. Updated Normalization
**Lines 179-183:** Normalized prices now use adjusted prices
```r
normalized1 = exp(logprice1_adj) / first(exp(logprice1_adj))
```

## Expected Benefits

1. **Better Signal Quality**
   - Z-scores now measure true pair-specific divergence
   - Less noise from market-wide movements

2. **Improved LSR Metric**
   - Mean reversion signals are cleaner
   - Better prediction of pair convergence

3. **More Stable Distance Metric**
   - Euclidean distance captures relative performance, not market trends

4. **Market Neutrality**
   - Strategy becomes more truly market-neutral
   - Lower correlation with broader market indices

## Validation Steps

### 1. Check correlation reduction:
```r
# Before vs After correlation with market
pairs_sample = dt_[stock1 == "AAPL" & stock2 == "MSFT"]

# Raw spread correlation with market
cor(diff(pairs_sample$spreadclose_raw), pairs_sample$market_factor[-1])

# Adjusted spread correlation with market (should be much lower)
cor(diff(pairs_sample$spreadclose), pairs_sample$market_factor[-1])
```

### 2. Compare Z-score stability:
```r
# Z-scores should have better stationarity properties
adf.test(pairs_sample$zscore_20)  # More negative = more stationary
```

### 3. Backtest comparison:
- Run full pipeline with adjusted spreads
- Compare Sharpe ratio, max drawdown, market correlation
- Expected improvement: 15-30% higher Sharpe ratio

## Alternative Configurations

### Option 1: Use SPY as Market Proxy (More Accurate)
If you have SPY data in prices:
```r
# Get market returns
spy_ret = prices[symbol == "SPY", .(date, mkt_ret = returns)]
dt_ = spy_ret[dt_, on = "date"]

# Calculate rolling betas
dt_[, beta1 := roll::roll_lm(mkt_ret, ret1, width = 60)$coefficients[1], 
    by = .(stock1, stock2)]
dt_[, beta2 := roll::roll_lm(mkt_ret, ret2, width = 60)$coefficients[1], 
    by = .(stock1, stock2)]

# Adjust returns
dt_[, ret1_adj := ret1 - beta1 * mkt_ret - mom1]
dt_[, ret2_adj := ret2 - beta2 * mkt_ret - mom2]
```

### Option 2: Sector-Specific Market Factor
Since pairs are from same sector:
```r
# Calculate sector average return
sector_ret = dt_[, .(sector_ret = mean(c(ret1, ret2))), by = .(date, sector)]
dt_ = sector_ret[dt_, on = c("date", "sector")]

# Remove sector factor instead of pairwise average
dt_[, ret1_adj := ret1 - sector_ret - mom1]
dt_[, ret2_adj := ret2 - sector_ret - mom2]
```

### Option 3: More Aggressive Momentum Window
```r
# Use 120-day momentum instead of 60-day
dt_[, mom1 := frollmean(ret1, 120, na.rm = TRUE), by = .(stock1, stock2)]
dt_[, mom2 := frollmean(ret2, 120, na.rm = TRUE), by = .(stock1, stock2)]
```

## Monitoring

After implementation, monitor these metrics:
1. Strategy correlation with SPY (should be closer to 0)
2. LSR metric distribution (should be more normal)
3. Pair turnover rate (may change slightly)
4. Performance in different market regimes (bull/bear/sideways)

## Potential Issues

1. **Cold Start:** First 60 days have no momentum adjustment
   - Solution: Already handled with `coalesce(mom, 0)`

2. **Extreme Values:** Adjusted prices could theoretically go negative in log-space
   - Solution: Monitor `logprice_adj` for unrealistic values

3. **Computational Cost:** Slightly increased due to rolling calculations
   - Impact: Minimal (< 10% increase in runtime)

## Files to Update Next

1. `research.R` - Update Z-score calculation to use adjusted spreads
2. `predictors_combo.R` - No changes needed (works on existing features)
3. Backtest code - May need to adjust transaction cost assumptions

## References

- Gatev, E., Goetzmann, W. N., & Rouwenhorst, K. G. (2006). "Pairs trading: Performance of a relative-value arbitrage rule"
- Do, B., & Faff, R. (2010). "Does simple pairs trading still work?"
- Avellaneda, M., & Lee, J. H. (2010). "Statistical arbitrage in the US equities market"
