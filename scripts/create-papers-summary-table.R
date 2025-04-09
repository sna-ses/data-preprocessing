library(arrow)
library(dplyr)

# tables ------------------------------------------------------------------
# main table
dp <- arrow::read_parquet("data/papers.parquet") |> 
  glimpse()

dpaa <- arrow::read_parquet("data/papers_auth_aff.parquet") |> 
  glimpse()

dpf <- arrow::read_parquet("data/papers_fields.parquet") |> 
  glimpse()

da <- arrow::read_parquet("data/affiliations.parquet") |> 
  select(affiliation_id, country_iso)

# summaries ---------------------------------------------------------------
dpaa_summ <- dpaa |>
  group_by(paper_id) |> 
  summarise(
    author_count = n_distinct(author_id),
    affiliation_count = n_distinct(affiliation_id)
  ) |> 
  collect() |> 
  glimpse()

dpf_summ1 <- dpf |> 
  filter(field_type == "Top") |> 
  group_by(paper_id) |> 
  summarise(top_field_count = n_distinct(field_name)) |> 
  collect()

dpf_summ2 <- dpf |> 
  filter(field_type == "Sub") |> 
  group_by(paper_id) |> 
  summarise(sub_field_count = n_distinct(field_name)) |> 
  collect()

int_collab <- dpaa |> 
  left_join(da, by = "affiliation_id") |> 
  group_by(paper_id) |> 
  summarize(countries_count = n_distinct(country_iso)) |>
  mutate(is_int_collab = if_else(countries_count > 1, TRUE, FALSE)) |> 
  collect()

# join --------------------------------------------------------------------
dp2 <- list(
  dp |> collect(),
  dpaa_summ,
  dpf_summ1,
  dpf_summ2,
  int_collab
) |> 
  purrr::reduce(left_join, by = join_by(paper_id))

dp2 |> glimpse()

dp2 |> count(original_venue)

# export ------------------------------------------------------------------
arrow::write_parquet(dp2, "data/papers_with_summaries.parquet")

# arrow::read_parquet("data/papers_with_summaries.parquet")
# arrow::open_dataset("data/papers_with_summaries.parquet")
