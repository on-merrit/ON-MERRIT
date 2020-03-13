library(tidyverse)
library(vroom)
library(lubridate)
library(patchwork)
library(ggforce)
library(ggridges)
library(gghalves)
source("WP3/Task3.1/R/00-functions.R")

df <- vroom(
  file = "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_austria_papers.csv",
  delim = ",",
  col_types = cols(
    paperid = col_number(),
    affiliationid = col_number(),
    normalizedname = col_character(),
    displayname = col_character(),
    wikipage = col_character(),
    normalizedwikiname = col_character(),
    year = col_date(format = "%Y"),
    count_OA_references = col_integer(),
    count_unknown_references = col_integer()
  ))


distinct_papers <- df %>% 
  distinct(paperid, year, count_OA_references, count_unknown_references) %>% 
  mutate(perc_oa = count_OA_references/(count_OA_references + count_unknown_references)) %>% 
  filter(year >= ymd("2007-01-01") & year <= ymd("2017-01-01"))

distinct_papers %>% 
  ggplot(aes(year, perc_oa)) +
  geom_jitter(size = .01, alpha = .1) +
  scale_y_binned()
  geom_smooth() 

  
# this is great but only with sampling
distinct_papers %>% 
  sample_n(10000) %>%
  ggplot(aes(year, y = perc_oa, group = year)) +
  geom_dotplot(binaxis = "y", binwidth = .005)
# I like this

# this is good too
distinct_papers %>% 
  # sample_n(10000) %>%
  mutate(year = year(year)) %>% 
  ggplot(aes(y = as.character(year), x = perc_oa, group = year)) +
  geom_density_ridges(quantile_lines = TRUE, scale = 1.2, fill = "#24A8AC",
                      alpha = .65) +
  scale_x_continuous(labels = scales::percent, limits = c(0, 1)) + 
  labs(y = NULL, x = "Percantage of papers that are OA") +
  theme_ridges()

distinct_papers %>% 
  sample_n(10000) %>%
  ggplot(aes(year, y = perc_oa, group = year)) +
  geom_dotplot(binaxis = "y", binwidth = .001) +
  coord_flip()

# this coneys the basic distributions
distinct_papers %>% 
  sample_n(10000) %>%
  ggplot(aes(year, perc_oa, group = year)) +
  geom_boxplot()


distinct_papers %>% 
  sample_n(10000) %>%
  ggplot(aes(year, perc_oa, group = year)) +
  geom_half_boxplot() +
  geom_half_violin(side = "r")
  
distinct_papers %>% 
  sample_n(10000) %>%
  ggplot(aes(year, perc_oa, group = year)) +
  geom_sina(alpha = .1)

geom_boxplot()
  # geom_hex(bins = 50) +
  geom_bin2d(bins = 50) +
  scale_fill_viridis_c() 
  geom_smooth(colour = "red")
  
  
  
  
# need more simple analysis: what is the distribution of references over the 
# papers?
distinct_papers %>% 
  pivot_longer(count_OA_references:count_unknown_references) %>% 
  ggplot(aes(value, fill = name)) +
  geom_density() +
  coord_cartesian(xlim = c(0, 200))

distinct_papers %>% 
  mutate(total_refs = count_OA_references + count_unknown_references) %>% 
  ggplot(aes(total_refs)) +
  geom_density(fill = "grey", adjust = .5) +
  coord_cartesian(xlim = c(0, 150))


