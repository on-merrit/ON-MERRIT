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
distinct_papers <- df %>% 
  distinct(paperid, year, count_OA_references, count_unknown_references) %>% 
  mutate(perc_oa = count_OA_references/(count_OA_references + count_unknown_references))

distinct_papers %>% 
  ggplot(aes(year, perc_oa)) +
  geom_jitter(size = .01, alpha = .1) +
  scale_y_binned()
  geom_smooth() 
  
distinct_papers %>% 
  # sample_n(10000) %>%
  ggplot(aes(year, perc_oa)) +
  geom_hex(bins = 50) +
  # geom_bin2d(bins = 50) +
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
