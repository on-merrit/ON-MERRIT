library(tidyverse)
library(vroom)
library(patchwork)
source("WP3/Task3.1/R/00-functions.R")

theme_set(theme_bw())

# function for saving the plots
write_plot <- function(plot, country) {
  ggsave(
    filename = glue::glue("WP3/Task3.1/output/Q7_figures/oa_development_{country}.png"),
    plot = plot, device = "png",
    width = 10, height = 10)
}

do_it <- function(path, country) {
  path %>% 
    get_data() %>% 
    plot_data(country = country) %>% 
    write_plot(country = country)
}

source_files <- tribble(
  ~country,   ~path,
  "Austria", "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_austria_papers.csv",
  "Brazil",  "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_brazil_papers.csv",
  "Germany", "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_germany_papers.csv",
  "India",  "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_india_papers.csv",
  "Portugal", "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_portugal_papers.csv",
  "Russia",  "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_russia_papers.csv",
  "the United Kingdom", "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_uk_papers.csv",
  "the United States", "WP3/Task3.1/data/processed/Q6_dataset/rc_oa_usa_papers.csv"
)

source_files %>% 
  as.list() %>% 
  pwalk(do_it)

