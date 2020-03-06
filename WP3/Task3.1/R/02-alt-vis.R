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

count(df, paperid, sort = T)

df %>% 
  distinct(paperid, year, count_OA_references, count_unknown_references) %>% 
  mutate(perc_oa = count_OA_references/(count_OA_references + count_unknown_references)) %>% 
  ggplot(aes(year, perc_oa)) +
  geom_jitter(size = .01, alpha = .1) +
  geom_smooth() 
