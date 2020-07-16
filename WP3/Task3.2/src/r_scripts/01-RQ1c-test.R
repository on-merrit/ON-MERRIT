library(tidyverse)

df <- read_csv("WP3/Task3.2/data/raw/author_ids_austria_papers.csv",
               col_types = cols(
                 affiliationid = col_character(),
                 paperid = col_character(),
                 year = col_character(),
                 authorid = col_character(),
                 normalizedname = col_character(),
                 displayname = col_character(),
                 wikipage = col_character(),
                 normalizedwikiname = col_character()
               ))


df_clean <- df %>% 
  filter(year != "year") %>% 
  mutate(year = as.integer(year)) %>% 
  distinct(paperid, year, authorid)



ages <- df_clean %>% 
  group_by(authorid) %>% 
  mutate(first_pub = min(year),
         current_acad_age = year - first_pub)


ages %>% 
  ggplot(aes(year)) +
  geom_density()
# almost no data before 1950, most of the data after 2000

ages_2000 <- ages %>% 
  filter(year > 2000) %>% 
  ungroup()
  
ages_2000 %>% 
  ggplot(aes(current_acad_age)) +
  geom_histogram() +
  facet_wrap(~year)

ages %>% 
  slice_sample(n = 10000) %>% 
  ggplot(aes(year, current_acad_age)) +
  geom_smooth()
# this is interesting. Why is the mean academic age at publication rising?

# overall: for our hypothesis we need to combine this with OA data, either
# on the article itself or the cited references (cant remember the hyp
# precisely)