library(readxl)
library(readr)
library(dplyr)
library(arrow)
library(stringr)

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
  filter(paper_id %in% collaborative_papers$paper_id) |> 
  distinct(paper_id, author_id, .keep_all = TRUE)

# índice de shannon para multidisciplina

multidiscipline_index <- collab_auth_field |> 
  count(paper_id, author_subfield) |> 
  group_by(paper_id) |> 
  summarise(shannon_index = -sum((n / sum(n)) * log(n / sum(n))), .groups = "drop") |> 
  rename(multidisciplinarity_messure = shannon_index) # renombramos la variable por si en el futuro se usa otra. Así no hay que editarla en la Shiny

# guardamos esta tabla como un archivo aparte porque lo usaremos para una visualización en particular
write_parquet(multidiscipline_index, "data/papers_multidicipline_index.parquet")


# Ahora veremos cuáles son las combinaciones de disciplinas que hay. La idea es tomar el top_field de cada paper y ver a qué top_field pertenecen los autores. 

paper_field <- read_parquet("data/papers_fields.parquet") |> 
  filter(paper_id %in% collaborative_papers$paper_id) |> 
  filter(field_type == "Top") |> 
  select(paper_id, paper_field = field_name) |> 
  left_join(select(collab_auth_field, paper_id, author_id, author_field = topfield_name), by = "paper_id") |> 
  distinct(paper_id, author_id, .keep_all = TRUE) 

# Le agregamos el año y quinquenio

opts_quinquenios <- c(
  "1990-1994", "1995-1999", "2000-2004", "2005-2009",
  "2010-2014", "2015-2019", "2020-2021"
)

quinquenio_breaks <- c(1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022)

paper_author_field <- read_parquet("data/papers.parquet") |> 
  select(paper_id, year) |> 
  filter(paper_id %in% collaborative_papers$paper_id) |> 
  mutate(quin = cut(
      year,
      breaks = quinquenio_breaks,
      labels = opts_quinquenios,
      right = FALSE,
      include.lowest = TRUE)
  ) |> 
  left_join(paper_field) |> 
  filter(!is.na(paper_field))

# Esta tabla intermedia la vamos a guardar por si acaso, pero por el momento no la utilizaremos directamente

write_parquet(paper_author_field, "data/papers_auth_fields.parquet")

# Creamos la tabla con los datos para cada combinación de disciplina/quinquenio

# paper_author_field <- read_parquet("data/papers_auth_fields.parquet")

fields_tables <- tidyr::crossing(field = unique(paper_author_field$paper_field), quin = opts_quinquenios)


create_fields_rds <- function(field, quin){
  
  table <-  paper_author_field |> 
    filter(paper_field == field & quin == {{ quin }}) |> 
    count(paper_field, author_field, sort = TRUE) |>
    filter(!is.na(author_field)) |> 
    rename(from = paper_field, to = author_field, weight = n) 
    

  saveRDS(table, stringr::str_glue("data/fields_auth_rds/{field}-{quin}.rds"))

}

purrr::walk2(fields_tables$field, fields_tables$quin, create_fields_rds)


