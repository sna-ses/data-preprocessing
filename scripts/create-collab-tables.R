library(dplyr)
library(tidyr)

# autores por paper
paper_authors <- arrow::read_parquet("data/papers_auth_aff.parquet") |> 
  select(paper_id, author_id)

# instituciones por paper

paper_institutions <- arrow::read_parquet("data/papers_auth_aff.parquet") |> 
  select(paper_id, affiliation_id) |> 
  distinct(paper_id, affiliation_id) |> 
  filter(!is.na(affiliation_id))

# para tener el año 
papers <- arrow::read_parquet("data/papers.parquet") |> 
  select(paper_id, year)

# id de papers con + de 1 autor
collaborative_papers <- arrow::read_parquet("data/papers_auth_aff.parquet") |> 
  distinct(paper_id, author_id) |> 
  count(paper_id, name = "authors_total") |> 
  filter(authors_total > 1) |> 
  left_join(papers)

# id de papers con + de 1 institución involucrada*
# * acá se podría hacer un filtro más fino quizás, porque no estamos controlando los casos de autores que tienen dos afiliaciones. ¿Las consideramos como colaboración igual?

multi_institution_papers <- arrow::read_parquet("data/papers_auth_aff.parquet") |> 
  distinct(paper_id, affiliation_id) |> 
  filter(!is.na(affiliation_id)) |> 
  count(paper_id, name = "affiliation_total") |> 
  filter(affiliation_total > 1) |> 
  left_join(papers)

# pares de autores por paper

authors_collab <- paper_authors |>
  filter(paper_id %in% collaborative_papers$paper_id) |> 
  reframe(author = combn(author_id, 2, simplify = FALSE), .by = paper_id) |> 
  unnest_wider(author, names_sep = "_") |> 
  left_join(papers) |> 
  mutate(paper_id = as.character(paper_id))

arrow::write_parquet(authors_collab, "data/collab_authors.parquet")


# pares de instituciones por paper

institutions_collab <- paper_institutions |> 
  filter(paper_id %in% multi_institution_papers$paper_id) |> 
  reframe(institution = combn(affiliation_id, 2, simplify = FALSE), .by = paper_id) |> 
  unnest_wider(institution, names_sep = "_") |> 
  left_join(papers) |> 
  mutate(paper_id = as.character(paper_id))

arrow::write_parquet(institutions_collab, "data/collab_institutions.parquet")
