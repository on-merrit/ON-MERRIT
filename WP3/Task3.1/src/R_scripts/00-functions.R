get_data <- function(file) {
  df <- vroom(
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
  
  df %>% 
    distinct(paperid, year, count_OA_references, count_unknown_references) %>% 
    mutate(perc_oa = count_OA_references/(count_OA_references + count_unknown_references)) %>% 
    filter(year >= ymd("2007-01-01") & year <= ymd("2017-01-01"))
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

