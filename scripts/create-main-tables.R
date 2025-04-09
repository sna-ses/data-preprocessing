library(arrow)
library(dplyr)
library(countrycode)

load("data/raw/b3_joined.RData")

# authors -----

authors <- b3_joined |> 
  select(AuthorID, Author_Name, H.index_auth, Productivity_auth, Average_C10_auth, Average_LogC10_auth) |> 
  distinct(AuthorID, .keep_all = TRUE) |> 
  janitor::clean_names() 

write_parquet(authors, "data/authors.parquet")

# affiliations ----

# se agregaron dos variables regionales: la de subregiones de naciones unidas (que agrupa Latinoamérica y el Caribe, por ejemplo) y otra que divide el mundo en 23 regiones (que separa Latam + Caribe en: Caribe, Centroamérica y Sudamérica). Luego podemos ver cuál es más relevante / útil
# se eliminaron las variables is_latam, latam y latam_prop porque el foco ahora es global y porque se puede calcular eso de las nuevas variables regionales 

affiliations <- b3_joined |> 
  select(AffiliationID, Affiliation_Name, GridID, Official_Page, country_iso = ISO3166Code, Latitude_aff, Longitude_aff, Productivity_aff, Average_C10_aff, Average_LogC10_aff, country_name = name) |> 
  distinct(AffiliationID, .keep_all = TRUE) |> 
  filter(!is.na(AffiliationID)) |> 
  janitor::clean_names() |> 
  mutate(subregion_un = countrycode(country_iso, origin = "iso2c", destination = "un.regionsub.name"),
         subregion_un = if_else(country_iso == "TW", "Eastern Asia", subregion_un),
         region = countrycode(country_iso, origin = "iso2c", destination = "region23"),
         region = if_else(country_iso == "TW", "Eastern Asia", region)) 
  
write_parquet(affiliations, "data/affiliations.parquet")


# main affiliation -----

author_main_aff <- readr::read_tsv("data/raw/afiliacion_principal_attr.tsv") |> 
  select(author_id = AuthorID, main_affiliation_id = AffiliationID)

write_parquet(author_main_aff, "data/author_main_aff.parquet")

# gender ----

author_gender <- readr::read_tsv("data/raw/gender_final.tsv") |> 
  select(AuthorID, `P(gf)`) |> 
  janitor::clean_names() |> 
  mutate(name_gender = case_when(
    p_gf >= 0.75 ~ "female",
    p_gf <= 0.25 ~ "male",
    .default = "indeterminate"
  ))

write_parquet(author_gender, "data/author_gender.parquet")

# author main field

author_main_field <- readr::read_tsv("data/raw/author_fields_merged.tsv") |> 
  select(author_id = AuthorID, author_main_field = field_predominant_all, field_type = type_predominant_all)

write_parquet(author_main_field, "data/author_main_field.parquet")


# papers  ----

# La variable DocType no se incluyó porque todas las observaciones tienen el mismo valor (Journal). Lo mismo respecto de ConferenceSeriesID (son todas NA)

papers <- b3_joined |> 
  select(PaperID, DOI, Year, Date, JournalID, OriginalVenue, ReferenceCount, CitationCount) |> 
  distinct(PaperID, .keep_all = TRUE) |> 
  janitor::clean_names()

write_parquet(papers, "data/papers.parquet")

# paper-author-affiliation ----

## todas las afiliaciones

papers_auth_aff <- b3_joined |> 
  select(PaperID, AuthorID, AffiliationID, AuthorSequenceNumber) |> 
  distinct(PaperID, AuthorID, AffiliationID, .keep_all = TRUE) |> 
  janitor::clean_names()

write_parquet(papers_auth_aff, "data/papers_auth_aff.parquet")


# titles ----

# Por tamaño el tamaño de archivo, es necesario que los títulos vayan en una tabla distinta

papers_titles <- b3_joined |> 
  select(PaperID, PaperTitle, Year) |> 
  distinct(PaperID, .keep_all = TRUE) |> 
  janitor::clean_names()


write_parquet(papers_titles, "data/papers_titles.parquet")

# papers fields ----

papers_fields <- b3_joined |> 
  select(PaperID, FieldID, Field_Name, Field_Type, Hit_1pct, Hit_5pct, Hit_10pct, C_f) |> 
  distinct(PaperID, FieldID, .keep_all = TRUE) |> 
  janitor::clean_names() 

write_parquet(papers_fields, "data/papers_fields.parquet")


# papers abstracts ----

papers_abstracts <- b3_joined |>
 select(PaperID, paper_abstract = ab, Year) |>
 distinct(PaperID, .keep_all = TRUE) |>
 janitor::clean_names() |>
 filter(!is.na(paper_abstract), paper_abstract != "")

# write_parquet(papers_abstracts, "data/papers_abstracts.parquet")

write_dataset(papers_abstracts, partitioning = "year", "data/abstracts", format = "parquet")

