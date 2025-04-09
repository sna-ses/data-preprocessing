library(readxl)
library(readr)
library(dplyr)
library(arrow)

# Tabla con correspondencia Top-Sub Fields

# read_excel("data/raw/FieldsSciSciNET.xlsx") |> 
#   select(subfield_name = sub_field, topfield_name = top_field) |> 
#   write_parquet("data/fields_sciscinet.xlsx")

top_sub <- read_parquet("data/fields_sciscinet.xlsx")

# Tabla main field autores y tabla con correspondencia código-nombre del field

author_field <- read_parquet("data/author_main_field.parquet") |> 
  select(author_id, author_subfield = author_main_field) |> 
  mutate(author_subfield = as.character(author_subfield))

subfields_ids <- read_parquet("data/papers_fields.parquet") |> 
  filter(field_type == "Sub") |> 
  distinct(field_id, field_name) |> 
  rename(subfield_id = field_id, subfield_name = field_name)

# Match Top-Sub

author_field <- author_field |> 
  left_join(subfields_ids, by = c("author_subfield" = "subfield_id")) |> 
  left_join(top_sub, by = "subfield_name") |> 
  mutate(author_id = as.character(author_id))

# Agregar info de fields de autores a los papers

papers_auth_field <- read_parquet("data/papers_auth_aff.parquet") |> 
  select(paper_id, author_id) |> 
  left_join(author_field)

# Quedarse solo con los papers colaborativos (> 1 autor)

collaborative_papers <- arrow::read_parquet("data/papers_auth_aff.parquet") |> 
  distinct(paper_id, author_id) |> 
  count(paper_id, name = "authors_total") |> 
  filter(authors_total > 1) |> 
  select(paper_id)

collab_auth_field <- papers_auth_field |> 
  filter(paper_id %in% collaborative_papers$paper_id)

# índice de shannon para multidisciplina

multidiscipline <- collab_auth_field |> 
  count(paper_id, author_subfield) |> 
  group_by(paper_id) |> 
  summarise(shannon_index = -sum((n / sum(n)) * log(n / sum(n))), .groups = "drop") |> 
  rename(multidisciplinarity_messure = shannon_index) # renombramos la variable por si en el futuro se usa otra. Así no hay que editarla en la Shiny


write_parquet(multidiscipline, "data/papers_multidicipline.parquet")
