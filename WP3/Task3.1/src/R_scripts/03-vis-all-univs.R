library(tidyverse)
library(vroom)
library(lubridate)
library(patchwork)
library(ggforce)
library(ggridges)
source("WP3/Task3.1/src/R_scripts/00-functions.R")


theme_set(theme_bw())

# function for saving the plots
write_plot <- function(plot, country) {
  ggsave(
    filename = glue::glue("WP3/Task3.1/output/Q7_figures/oa_per_uni_{country}.png"),
    plot = plot, device = "png",
    width = 10, height = 10)
}

save_data <- function(df, country) {
  df %>% 
    oa_quartiles() %>% 
    pivot_longer(-year, names_to = "quantile") %>%
    write_csv(
      glue::glue("WP3/Task3.1/data/processed/Q7_dataset/quantiles_{country}.csv")
    )
  
  write_csv(
    data_per_university(df) %>% 
      rename(univ_name = displayname),
    glue::glue("WP3/Task3.1/data/processed/Q7_dataset/universities_{country}.csv")
  )
  
  invisible(df)
}


plot_median <- function(df, country) {
  # top part of plot
  top_plot <- df %>% 
    oa_quartiles() %>% 
    pivot_longer(-year) %>% 
    ggplot(aes(year, value, group = name)) +
    geom_line() +
    scale_y_continuous(labels = scales::percent) +
    coord_cartesian(ylim = c(0, .4)) +
    labs(x = NULL, y = "% of references that are OA",
         title = glue::glue("Quartiles of OA percentage for {country}"),
         subtitle = "Quartiles: q25, q50, q75")
  
  # bottom part
  df_per_uni <- data_per_university(df)
  
  top_univs <- df_per_uni %>% 
    ungroup() %>% 
    filter(year == ymd("2017-01-01")) %>% 
    arrange(desc(median_oa_perc)) %>% 
    top_n(5)
  
  
  bottom_plot <- df_per_uni %>% 
    anti_join(top_univs, by = "displayname") %>% 
    ggplot(aes(year, median_oa_perc, group = displayname)) +
    geom_line(colour = "grey80") +
    geom_line(data = filter(df_per_uni, displayname %in% top_univs$displayname),
              aes(colour = displayname),
              size = .8) +
    scale_y_continuous(labels = scales::percent) +
    coord_cartesian(ylim = c(0, .35)) +
    labs(x = NULL, 
         title = glue::glue("Median of OA reference proportion for {country}"),
         colour = NULL, y = NULL) +
    theme(legend.position = "top")
  
  
  top_plot / bottom_plot +
    plot_layout(heights = c(1, 2))
}


do_it <- function(path, country) {
  path %>% 
    import_data() %>% 
    save_data(country = country) %>% 
    plot_median(country = country) %>% 
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


