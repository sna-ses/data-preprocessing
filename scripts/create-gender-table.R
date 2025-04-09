library(arrow)
library(dplyr)
library(tidyr)


author_sequence_number <- read_parquet("data/papers_auth_aff.parquet") |> 
  select(-affiliation_id) |> 
  distinct(paper_id, author_id, author_sequence_number) # para evitar repeticiones por doble afiliación

authors_data <- read_parquet("data/authors_with_extra_data.parquet") |> 
  mutate(author_id = as.character(author_id))

papers_data <- read_parquet("data/papers_with_summaries.parquet") 

authors_gender <- author_sequence_number |> 
  left_join(select(authors_data, author_id, name_gender))

first_author <- authors_gender |>
  filter(author_sequence_number == 1) |>
  select(paper_id, gender_first_auth = name_gender) |> 
  mutate(gender_first_auth = factor(gender_first_auth, levels = c("female", "male", "indeterminate")))


total_authors <- authors_gender |> 
  select(paper_id, author_sequence_number) |> 
  slice_max(author_sequence_number, by = paper_id) |> 
  rename(n_authors = author_sequence_number) |> 
  mutate(is_collab = if_else(n_authors > 1, TRUE, FALSE))

paper_gender <- authors_gender |> 
  filter(!is.na(name_gender)) |> 
  count(paper_id, name_gender) |> 
  complete(paper_id, name_gender, fill = list(n = 0)) |> 
  arrange(paper_id) |> 
  left_join(total_authors, by = "paper_id") |> 
  mutate(name_gender = factor(name_gender, levels = c("female", "male", "indeterminate"))) |> 
  left_join(first_author, by = "paper_id")

# gender_collab <- authors_gender |> 
#   filter(is_collab == TRUE) |> 
#   mutate(percent_gender = n / n_authors * 100)

write_parquet(paper_gender, "data/papers_gender.parquet")

paper_gender |> 
  group_by(name_gender) |> 
  summarise(author_gender = sum(n))
