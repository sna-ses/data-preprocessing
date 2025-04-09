# author jbkunst
library(arrow)
library(dplyr)

# join --------------------------------------------------------------------
d <- read_parquet("data/authors.parquet") |> 
  select(author_id) |> 
  mutate(author_id = as.numeric(author_id))

d <- d |> 
  # author_main_field
  left_join(
    read_parquet("data/author_main_field.parquet") |>
      select(author_id, author_main_field),
    by = join_by(author_id)
  ) 

d <- d |> 
  left_join(
    read_parquet("data/papers_fields.parquet") |> 
      select(field_id, field_name) |> 
      distinct(field_id, field_name) |> 
      mutate(field_id = as.numeric(field_id)),
    by = join_by(author_main_field == field_id)
  )

d <- d |> 
  left_join(
    read_parquet("data/author_gender.parquet"),
    by = join_by(author_id)
  ) 

d <- d |> 
  left_join(
    read_parquet("data/author_main_aff.parquet"),
    by = join_by(author_id)
  )

d <- d |> 
  left_join(
    read_parquet("data/affiliations.parquet") |> 
      select(affiliation_id, affiliation_name, country_name, subregion_un, region),
    by = join_by(main_affiliation_id == affiliation_id)
  )

# join with original table ------------------------------------------------
d <- read_parquet("data/authors.parquet") |> 
  mutate(author_id = as.numeric(author_id)) |> 
  left_join(d, by = join_by(author_id))

d

glimpse(d)

# export ------------------------------------------------------------------
arrow::write_parquet(d, "data/authors_with_extra_data.parquet")
