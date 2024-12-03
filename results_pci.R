library(data.table)


# Import meta
meta = fread("F:/strategies/statsarb/pci/meta.csv")

# Import pci tests
files = list.files("F:/strategies/statsarb/pci/output_pci", full.names = TRUE)
indecies_meta = basename(files)
pci_tests = lapply(files, fread)
pci_tests = rbindlist(pci_tests, fill = TRUE)

# Apply restrictions to the universe
# 1) pairs with a half-life of mean-reversion of one day or less - thereby avoiding to select
#where trading gains are largely attributable to bid-ask bounce
# 2) pvmr > 0.5 ensures    pairs  more reliable parameter estimates
# 3) p_rw < 0.05 & p_ar < 0.05. A time series is classified as partially cointegrated,
#    if and only if the random walk as well as the AR(1)-hypotheses are rejected
# 3) my condition: MR p value should be lower than 0.05 because this test confirms mean reverting component
# 4) restriction to same sector I DON'T WANT APPLY THIS FOR  NOW
# 5) 25% lowest  by neLog
# 6) possible to add additional fundamental matching
pci_tests_eligible = pci_tests[pvmr > 0.5  & rho > 0.5 & p_rw < 0.05 & p_ar < 0.05 &
                                 negloglik <= quantile(negloglik, probs = 0.25)] # 1), 2), 3), 5)

# remove same pairs
pci_tests_eligible = pci_tests_eligible[!(series_2 %in% series_1)]

# example of partially cointegrated pairs
pairs = unlist(pci_tests_eligible[1, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2])) # log prices series of first pair
# EQIX - Equinix, Inc. je američka multinacionalna tvrtka sa sjedištem u Redwood Cityju, Kalifornija,
#        specijalizirana za internetske veze i podatkovne centre.
# AMT - American Tower Corporation je američka zaklada za ulaganje u nekretnine te vlasnik i operater bežične i
#       televizijske komunikacijske infrastrukture u nekoliko zemalja
pairs <- unlist(pci_tests_eligible[2, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2])) # log prices series of second pair
# DOV - američki je konglomerat proizvođača industrijskih proizvod
# APH - glavni proizvođač elektroničkih i optičkih konektora, kabelskih i interkonektnih sustava kao što su koaksijalni kabeli
pairs = unlist(pci_tests_eligible[3, 1:2])
plot(log(train[, pairs]), main = paste0(pairs[1], " - ", pairs[2])) # log prices series of third pair
# O - investicijski fond za nekretnine
# CPT - investicijski fond za nekretnine
