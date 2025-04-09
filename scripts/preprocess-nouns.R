library(udpipe)
library(cld2) 
library(dplyr)
library(arrow)
library(stringr)


# 1. Detectar idioma de los abstracts y filtrar los escritos en inglés

abstracts_en <- read_parquet("data/papers_abstracts.parquet") |> 
  mutate(lang = detect_language(paper_abstract)) |> 
  filter(lang == "en")

# descargar modelo (elegimos https://gucorpling.org/gum/)

udpipe_download_model(language = "english-gum", model_dir = "data")


eng_model <- udpipe_load_model("data/english-gum-ud-2.5-191206.udpipe")


# 2. crear una tabla con los sustantivos de cada paper en inglés

get_nouns <- function(abstract, id){
  
  nouns <- udpipe_annotate(abstract, object = eng_model, doc_id = id) |> 
    as_tibble() |> 
    filter(upos %in% c("NOUN")) |> 
    select(paper_id = doc_id, lemma) |> 
    count(paper_id, lemma)
  
  readr::write_csv(nouns, str_glue("data/abstracts_nouns/{id}.csv"))
}

# NOTA: este proceso es lento. Dependiendo de las características de la máquina en que se ejecute, puede tomar un par de días
purrr::walk2(abstracts_en$paper_abstract, abstracts_en$paper_id, get_nouns)


# 3. crear una tabla con los sustantivos de cada top field

fields <- read_parquet("data/papers_fields.parquet")

papers <- read_parquet("data/papers.parquet") |> 
  select(paper_id, year)

top_fields <- fields |>
  filter(field_type == "Top") |>
  select(paper_id:field_name) |>
  left_join(papers)

files <- top_fields |> 
  mutate(file = str_glue("data/abstracts_nouns/{paper_id}.csv")) 


# por si acaso
possibly_read_csv <- possibly(read_csv)


create_field_table <- function(field){
  
  filtered_table <- files |> 
    filter(field_name == field)
  
  field_nouns <- map(filtered_table$file, \(x) possibly_read_csv(x, show_col_types = FALSE, col_types = "cci"))  |> 
    list_rbind() |> 
    left_join(top_fields)
  
  field_name <- janitor::make_clean_names(field)
  
  arrow::write_parquet(field_nouns, str_glue("data/fields_nouns/{field_name}_nouns.parquet"))
  
}


fields_names <- unique(top_fields$field_name)

walk(fields_names, create_field_table)


  


     