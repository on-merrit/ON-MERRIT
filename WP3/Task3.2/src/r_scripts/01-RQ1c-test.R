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


# New approach with correct year of first paper -----
df_full <- read_csv("WP3/Task3.2/data/raw/oa_authors_austria.csv")

df_full_clean <- df_full %>% 
  filter(!is.na(paperid)) # filter out headers

# calculate age
with_age <- df_full_clean %>% 
  mutate(age_at_publication = year - first_paper)

with_age %>% 
  ggplot(aes(age_at_publication)) +
  geom_histogram()
# we might have errors here

with_age %>% 
  filter(age_at_publication > 60) %>% 
  arrange(desc(age_at_publication)) 
# clearly there are issues with author disambiguation, i.e. a paper being
# attributed to ludiwg boltzmann from 1979
# use the median for now

with_age %>% 
  ggplot(aes(age_at_publication)) +
  geom_histogram(binwidth = 1) +
  coord_cartesian(xlim = c(0, 60))
# a great many papers have age 0 at publication (more than 100.000). 
# is this because they only published this one paper, or is this another error

# maybe first look only at the years we are interested, to avoid errors for data
# from 1900 which we dont care about

our_sample <- with_age %>% 
  filter(year %in% 2007:2017)

our_sample %>% 
  ggplot(aes(age_at_publication)) +
  geom_histogram(binwidth = 1) +
  coord_cartesian(xlim = c(0, 60))
# this is similar

our_sample %>% 
  filter(age_at_publication == 0) %>% 
  arrange(year)
# need to check whether this is genuine or an error
# then check age groups


# check age groups
our_sample %>% 
  group_by(year) %>% 
  summarise(med_age = median(age_at_publication),
            sd = sd(age_at_publication),
            n = n())

p <- our_sample %>% 
  ggplot(aes(year, age_at_publication, group = year)) +
  geom_boxplot()
p

p + coord_cartesian(ylim = c(0, 60))


# age group per year
p1 <- our_sample %>% 
  count(year, age_at_publication) %>% 
  ggplot(aes(age_at_publication, as.factor(year), fill = n)) +
  geom_tile() +
  scale_fill_viridis_c() +
  scale_x_continuous(breaks = scales::pretty_breaks(10)) +
  theme(legend.position = "top") +
  labs(y = NULL)
p1
p1 + coord_cartesian(xlim = c(0, 60))

# why are there these bands? maybe there is still some duplication here?
set_2017 <- our_sample %>% 
  filter(year == 2017, age_at_publication %in% 100:110)

set_2016 <- our_sample %>% 
  filter(year == 2016, age_at_publication %in% 100:110)
# there are some similarities here: some authors (anton luger, christian urban,
# wolfgang schwinger) have a wrong first year, but they keep publishing.
# Therefore, this distinct group of authors grows older, and is offset each year
# they come mainly from medical universities of vienna and graz
# maybe some ids are not unique?
# Input Ilire: author disambiguation is likely the culprit
# solution: take on of these authors, and interactively look at those papers
# 
# this can be done interactively, to some degree, via microsoft academic web
# for anton luger: https://academic.microsoft.com/author/2112108339/
# this only starts in 1982, so why do we have 1910 here? need to look into the
# part where we calculate first paper
# solution: run the code only for this single id
# also: read this: https://www.microsoft.com/en-us/research/project/academic/
# articles/microsoft-academic-uses-knowledge-address-problem-conflation-disambiguation/

# look into oa ----
oa_percentage <- our_sample %>% 
  mutate(is_OA = case_when(is_OA ~ TRUE,
                           TRUE ~ FALSE)) %>% 
  group_by(year, age_at_publication) %>% 
  summarise(oa_perc = mean(is_OA))

p <- oa_percentage %>% 
  ggplot(aes(age_at_publication, as.factor(year), fill = oa_perc)) +
  geom_tile() +
  scale_fill_viridis_c()
p

p + scale_x_continuous(limits = c(0, 60))


p + scale_x_binned(breaks = seq(0, 60, by = 3), limits = c(0, 60))

p + scale_x_binned(n.breaks = 20)


oa_percentage %>% 
  ggplot(aes(as.factor(year), oa_perc)) +
  geom_boxplot()

# over age
oa_percentage %>% 
  ggplot(aes(age_at_publication, oa_perc, group = age_at_publication)) +
  geom_boxplot()

oa_percentage %>% 
  ggplot(aes(age_at_publication, oa_perc)) +
  geom_smooth() +
  coord_cartesian(xlim = c(0, 60))
