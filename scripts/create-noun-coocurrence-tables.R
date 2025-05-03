# Datos para gráfico de co-ocurrencia

library(dplyr)
library(arrow)
library(readr)
library(stringr)
library(purrr)
library(tidyr)
library(data.table)


opts_quinquenios <- c(
  "1990-1994", "1995-1999", "2000-2004", "2005-2009",
  "2010-2014", "2015-2019", "2020-2021"
)


## PARTE 1: Calculamos las co-ocurrencias más frecuentes 

source("scripts/stopwords.R") # para cargar el objeto con las stopwords

# Guardamos las rutas de las tablas con todos los nouns de cada field

paths <- list.files("data/fields_nouns", full.names = TRUE)


# Creamos una función que lea el archivo de cada ruta calcule las coocurrencias por quinquenio y guarde el top 30

get_top_cooccurr <- function(path) {
  all_nouns <- arrow::read_parquet(path) |>
    mutate(lemma = str_to_lower(lemma)) |> # just in case
    filter(!lemma %in% stopwords$stopword) |>
    filter(!str_detect(lemma, "^\\.")) # quedaban cosas

  # guardamos el nombre del field para usarlo más adelante al guardar la tabla
  field <- unique(all_nouns$field_name)[1]

  selected_nouns <- all_nouns |>
    filter(str_detect(lemma, "sustaina")) |>
    distinct(paper_id) |>
    left_join(all_nouns)

  # De aquí en adelante usamos data.table porque es más rápido y evita que el proceso tome demasiado tiempo (o simplemente no corra) en computadores que no tengan demasiada memoria RAM

  nouns_dt <- data.table(selected_nouns)

  # nos quedamos solo con los años de interés (este bloque de código y el siguiente habría que editarlos si se agregan más datos en el futuro):

  nouns_dt <- nouns_dt[year >= 1990 & year <= 2021]

  # agregamos el quinquenio como variable:
  source("scripts/stopwords.R") # para cargar el objeto con las stopwords

  quinquenio_breaks <- c(1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022)

  nouns_dt[, quin := cut(year,
    breaks = quinquenio_breaks,
    labels = opts_quinquenios,
    right = FALSE,
    include.lowest = TRUE
  )]

  # hacemos uncount() en versión data.table

  expanded_dt <- nouns_dt[, .(lemma = rep(lemma, n), quin = unique(quin)), by = paper_id]

  # generamos los pares de co-ocurrencias por paper
  pairs_dt <- expanded_dt[,
    {
      target_lemmas <- lemma[grepl("^sustaina", lemma)]
      other_lemmas <- lemma
      # generar combinaciones
      pairs <- CJ(lemma_1 = target_lemmas, lemma_2 = other_lemmas)[lemma_1 != lemma_2]

      pairs[, quin := unique(quin)]
    },
    by = paper_id
  ]

  pairs_dt[, lemma_1 := "sustainability"]
  
  # contamos los pares
  pairs_count <- pairs_dt[, .(freq = .N), by = .(quin, lemma_1, lemma_2)]

  # dejamos de usar data.table por ahora y nos quedamos solo con los 30 más frecuentes
  file_name <- str_replace(basename(path), "nouns", "cooccurrences")

  pairs_count |>
    slice_max(freq, n = 30, by = quin) |> # si quisieran cambiar la cantidad de palabras que se muestran
    mutate(top_field = field) |>
    write_parquet(str_glue("data/fields_nouns_top_cooccurr/{file_name}"))
}

purrr::walk(paths, get_top_cooccurr)

## PARTE 2: Creamos las tablas preprocesadas que luego importaremos a la Shiny


# Guardamos las rutas de las tablas con todos los nouns de cada field

paths <- list.files("data/fields_nouns_top_cooccurr", full.names = TRUE)

# Creamos las combinaciones posibles de tablas y quinquenios
cooccurr_tables <- tidyr::crossing(path = paths, quin = opts_quinquenios)

create_cooccurr_rds <- function(path, quin) {
  
cooccurr <- read_parquet(path) |> 
  filter(quin == {{ quin }}) 


nodes_freq <- cooccurr %>%
  select(lemma_1, lemma_2, freq) %>%
  pivot_longer(cols = c(lemma_1, lemma_2), names_to = "type", values_to = "lemma") %>%
  summarize(freq_total = sum(freq), .by = lemma)

nodes <- nodes_freq |> 
  mutate(label = lemma, .after = lemma) |> 
  rename(id = lemma, value = freq_total) |> 
  mutate(value_log = log1p(value))
  
edges <-   tibble(
  from = cooccurr$lemma_1,
  to = cooccurr$lemma_2,
  value = cooccurr$freq,  
  title = paste("Frequency:", cooccurr$freq)
)

# save rds files

field = cooccurr$top_field[1]

saveRDS(edges, str_glue("data/cooccurrence_rds/edges_{field}-{quin}.rds"))
saveRDS(nodes, str_glue("data/cooccurrence_rds/nodes_{field}-{quin}.rds"))


}


purrr::walk2(cooccurr_tables$path, cooccurr_tables$quin, create_cooccurr_rds)

