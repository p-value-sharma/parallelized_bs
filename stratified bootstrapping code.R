library(purrr)
library(dplyr)
library(broom)
library(parallel)

# creating fake data ####
set.seed(3)
n_regions = 80
outcome <- unlist(lapply(1:n_regions, function(x) rnorm(n = 20, mean = 1000, sd = 100)))
regions <- rep(paste('r', sample(x = 1:200, size = n_regions, replace = F), sep = '_'), each = n_regions)
interv_era <- c(rep(0, 10), rep(1, 10))
df <- data.frame(region = regions, outcome, interv_era)
# x is region, j is era, k is n_obs in that era


# parallelized bootstrapping and model fitting ####
df0 <- subset(df, select = -outcome) 
n_pre_obs <- 10
n_post_obs <- 10

num_cores <- detectCores()
cl <- makeCluster(num_cores)
clusterExport(cl, varlist=c("df", 'df0', 'n_pre_obs', 'n_post_obs')) 
clusterSetRNGStream(cl = cl, iseed = 42)
n_bootstrap <- 10000

system.time(expr = {result <- parLapply(cl, X = 1:n_bootstrap, fun = function(h) {
    # x is the region, j is the interv_era, k is the number of_obs in that era to resample
    bs <- mapply(
      FUN =  function(x, j, k) {
        sample(df$outcome[df$region == x &
                            df$interv_era == j], k, replace = T)},
      x = as.character(rep(unique(df$region), each = 2)),
      j = c(0, 1),
      k = c(n_pre_obs, n_post_obs),
      SIMPLIFY = F
    )
    df0$bs <- as.vector(unlist(bs))
    return(df0)
  }) 

model_outputs_list <-
  parLapply(
    cl,
    result,
    fun = function(x) {
      broom::tidy(lm(bs ~ interv_era, data = x))
    }
  )})

stopCluster(cl)



# cleaning the output for readability####
cis <- model_outputs_list %>% 
  # this combines all the lists
  data.table::rbindlist() %>% 
  # convert the data.table to a tibble
  tbl_df() %>% 
  # choosing only the relevant columns
  select(term, estimate) %>% 
  # nest rolls up the values based on our selected group, which is the model paramters 
  tidyr::nest(-term) %>% 
  # this calculates the 2.5% and 97.% quantiles
  mutate(Quantiles = map(data, ~ quantile(.$estimate, probs = c(0.025,0.975)))) %>% 
  tidyr::unnest(map(Quantiles, tidy)) %>% 
  # spread makes the 95CIs more readable
  tidyr::spread(key = names, value = x)

