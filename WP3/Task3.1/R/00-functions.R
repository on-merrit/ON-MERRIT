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
              n_total = n_oa + n_unknown,
              perc_oa = n_oa / (n_oa + n_unknown)) %>% 
    ungroup()
  
}

plot_data <- function(df, country) {
  # set alpha according to number of universities
  n_universities <- length(unique(df$displayname))
  
  alpha <- (1/n_universities)^(1/6)
  
  perc_plot <- df %>% 
    ggplot(aes(year, perc_oa)) +
    geom_line(aes(colour = displayname, group = displayname),
              alpha = alpha, show.legend = FALSE) +
    geom_smooth() +
    scale_y_continuous(labels = scales::percent) +
    coord_cartesian(ylim = c(0, .6)) +
    labs(x = NULL, y = "% of papers that are OA", colour = "University",
         title = glue::glue("OA percentages for {country}"))
  
  abs_plot <- df %>% 
    group_by(year) %>% 
    summarise(OA = sum(n_oa),
              Unknown = sum(n_unknown),
              Total = sum(n_total)) %>% 
    pivot_longer(
      cols = OA:Total,
      names_to = "type",
      values_to = "value"
    ) %>% 
    mutate(value = value/1000) %>% 
    ggplot(aes(year, value, colour = type, group = type)) +
    geom_line() +
    labs(x = NULL, y = "# of published papers (in thousands)", colour = NULL,
         title = glue::glue("Number of published papers per year for {country}")) +
    theme(legend.position = c(.2, .8)) +
    scale_color_manual(values = c(Total = "black", OA = "red", Unknown = "blue"))
  
  abs_plot / perc_plot
}

