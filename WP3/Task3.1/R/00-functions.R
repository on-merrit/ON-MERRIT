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
    # sometimes the column headings are repeated. this removes those rows
    filter(!is.na(paperid)) %>% 
    group_by(displayname, year) %>% 
    summarise(n_oa = sum(count_OA_references),
              n_unknown = sum(count_unknown_references),
              perc_oa = n_oa / (n_oa + n_unknown)) %>% 
    ungroup()
  
}

plot_data <- function(df, country) {
  # set alpha according to number of universities
  n_universities <- length(unique(df$displayname))
  
  alpha <- (1/n_universities)^(1/6)
  
  df %>% 
    ggplot(aes(year, perc_oa, colour = displayname, group = displayname)) +
    geom_line(alpha = alpha, show.legend = FALSE) +
    scale_y_continuous(labels = scales::percent) +
    coord_cartesian(ylim = c(0, .6)) +
    labs(x = NULL, y = "% of papers that are OA", colour = "University",
         title = glue::glue("OA percentages for {country}"))
}
