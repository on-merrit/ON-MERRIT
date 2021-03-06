# this script servers as a central source for code which is re-used in several
# other scripts.
import_data <- function(file) {
  vroom(
    file = file,
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
  
}

get_distinct_papers <- function(df) {
  df %>% 
    distinct(paperid, year, count_OA_references, count_unknown_references) %>% 
    mutate(perc_oa = count_OA_references/(count_OA_references + count_unknown_references)) %>% 
    filter(year >= ymd("2007-01-01") & year <= ymd("2017-01-01"))
}

data_per_university <- function(df) {
  df %>% 
    filter(year >= ymd("2007-01-01") & year <= ymd("2017-01-01")) %>% 
    mutate(perc_oa = count_OA_references/(count_OA_references + count_unknown_references)) %>% 
    group_by(displayname, year) %>% 
    summarise(median_oa_perc = median(perc_oa)) %>% 
    ungroup() %>% 
    mutate(displayname = str_wrap(displayname, 30))
}

oa_quartiles <- function(df) {
  df %>% 
    distinct(paperid, year, count_OA_references, count_unknown_references) %>% 
    filter(year >= ymd("2007-01-01") & year <= ymd("2017-01-01")) %>% 
    mutate(perc_oa = count_OA_references/(count_OA_references + count_unknown_references)) %>% 
    group_by(year) %>% 
    summarise(q25 = quantile(perc_oa, .25),
              q50 = quantile(perc_oa, .5),
              q75 = quantile(perc_oa, .75))
}

plot_data <- function(df, country) {
  perc_plot <- df %>% 
    mutate(year = year(year)) %>% 
    ggplot(aes(y = as.character(year), x = perc_oa, group = year)) +
    geom_density_ridges(quantile_lines = TRUE, scale = 1.2, fill = "#24A8AC",
                        alpha = .65) +
    scale_x_continuous(labels = scales::percent, limits = c(0, 1)) + 
    labs(y = NULL, x = "% of references that are OA",
         title = glue::glue("OA percentages of references for {country}")) +
    theme_ridges()
  
  abs_plot <- df %>% 
    count(year) %>% 
    ggplot(aes(year, n)) +
    geom_line() +
    geom_point() +
    scale_y_continuous(labels = scales::label_comma()) +
    labs(x = NULL, y = "# of published papers", colour = NULL,
         title = glue::glue("# of published papers per year for {country}")) +
    theme(legend.position = c(.2, .8)) 
  
  abs_plot / perc_plot +
    plot_layout(heights = c(1, 2))
}

