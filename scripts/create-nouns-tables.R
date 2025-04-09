# Crear tabla de nouns de cada field por quinquenio

library(dplyr)
library(arrow)
library(stringr)
library(rlang)
library(tidyr)

source("global.R") # para cargar el objeto con los quinquenios
source("R/stopwords.R") # para cargar el objeto con las stopwords

# Guardamos las rutas de las tablas con todos los nouns de cada field

paths <- list.files("data/fields_nouns", full.names = TRUE)

# Creamos las combinaciones posibles de tablas y quinquenios
nouns_tables <- tidyr::crossing(path = paths, quin = opts_quinquenios)


# Creamos una función que lea el archivo de cada ruta, lo filtre según el quinquenio y guarde las 100 palabras más frecuentes en ese quinquenio

get_top_nouns <- function(path, quin) {

  nouns_freq <- read_parquet(path) |> 
    select(lemma, n, field_name, year) |> 
    filter(year %in% eval(parse_expr(str_replace(quin, "-", ":")))) |> 
    mutate(quin = quin) |> 
    filter(str_length(lemma) > 2,
           !lemma %in% stopwords$stopword) |> 
    summarise(n = sum(n), .by = c(lemma, quin, field_name))
  
  total_nouns <- sum(nouns_freq$n)
  
  top <- nouns_freq |>
    slice_max(n, n = 100, by = quin) |> # acá se puede cambiar el número de palabras que mostrar
    mutate(rel_freq = n / total_nouns)
  
  field <- unique(top$field_name)[1]
  
  top |> 
    select(lemma, n, rel_freq) |> 
    saveRDS(str_glue("data/nouns_rds/{field}-{quin}.rds"))
  
}

# Iteramos por el dataframe con las combinaciones para generar cada tabla

purrr::walk2(nouns_tables$path, nouns_tables$quin, get_top_nouns)


