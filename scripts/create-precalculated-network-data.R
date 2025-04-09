# load options and packages
source("global.R")

tidyr::crossing(field = opts_top_fields, quin = opts_quinquenios) |> 
  # sample_frac(1) |> 
  pwalk(function(field, quin){
    
    # field <- "Art"
    # quin <- "2000-2004"
    
    # field <- "Business"
    # quin <- "2005-2009"

    fout <- str_glue("data/net_rds/{field}-{quin}.rds")
    if(file.exists(fout)) return(TRUE)    
    
    cli::cli_h1("{field} - {quin}")
    cli::cli_progress_step("{field} - {quin}")
    
    # dat_papers --------------------------------------------------------------
    cli::cli_inform("`dat_papers`")
    # same as servers dat_papers2()
    dat_papers <- table("papers_with_summaries")
    
    y1 <- as.numeric(str_split(quin, "-")[[1]][1])
    y2 <- as.numeric(str_split(quin, "-")[[1]][1])
  
    dat_papers <- dat_papers |> 
      filter(y1 <= year, year <= y2)
    
    # filter by top field
    dat_papers_fields <- table("papers_fields")
    
    dat_papers_fields <- dat_papers_fields |> 
      filter(field_name %in% field) |> 
      select(paper_id, field_id, field_name, field_type)
    
    dat_papers <- semi_join(
      dat_papers,
      dat_papers_fields,
      by = join_by(paper_id)
    )

    # dat_collab --------------------------------------------------------------
    cli::cli_inform("`dat_collab`")
    
    dat_collab <- table("collab_authors2") |>
      semi_join(dat_papers, by = join_by(paper_id)) |>
      left_join(
        table("papers_with_summaries") |> select(paper_id, year, author_count),
        by = join_by(paper_id, year)
      ) |>
      collect() |>
      mutate(weight = 1/(author_count * (author_count-1)/2)) |> 
      collect() |> 
      group_by(author_1, author_2) |> 
      summarise(
        fractional_weight = sum(weight),
        weigth = n(),
        # papers = list(paper_id)
        ) |> 
      ungroup() |> 
      arrange(desc(weigth)) |> 
      collect()

    dat_collab <- dat_collab |> 
      rename(from = author_1, to = author_2)
    
    dat_collab |> 
      arrange(desc(fractional_weight))
    
    # g -----------------------------------------------------------------------
    cli::cli_inform("`g`")    
    g <- graph_from_data_frame(dat_collab |> select(from, to, weigth = fractional_weight), directed = FALSE)
    g2<- graph_from_data_frame(dat_collab |> select(from, to, weigth = weigth), directed = FALSE)
    
    E(g)$weigth
    E(g2)$weigth
    
    #--- Degree ponderado ---
    # Este será simplemente la suma de pesos de las conexiones (strength):
    weighted_degree <- strength(g , mode = "all", weights = E(g)$weigth)
    normal_degree   <- strength(g2, mode = "all", weights = E(g2)$weigth)
    normal_degree2  <- strength(g,  mode = "all")
    
    all(names(weighted_degree) == names(normal_degree))

    #--- Betweenness ponderado ---
    # En betweenness, el peso es interpretado como distancia o costo:
    weighted_betweenness <- betweenness(g , directed = FALSE, weights = 1/E(g )$weigth, normalized = FALSE)
    normal_betweenness   <- betweenness(g2, directed = FALSE, weights = 1/E(g2)$weigth, normalized = FALSE)

    all(names(weighted_betweenness) == names(normal_betweenness))
    all(weighted_betweenness == normal_betweenness) 

    # Unir resultados en un dataframe final:
    # resultados <- tibble(
    #   autor = names(weighted_degree),
    #   weighted_degree = weighted_degree,
    #   normal_degree = normal_degree,
    #   normal_degree2 = normal_degree2,
    #   weighted_betweenness = weighted_betweenness,
    #   normal_betweenness = normal_betweenness
    # )
    # resultados |> filter(weighted_betweenness != normal_betweenness)
    # 
    # resultados |> filter(autor == "1979445102")
    # dat_collab |> filter(from == "1979445102" | to == "1979445102")

    # Métricas básicas
    V(g)$betweenness <- betweenness(g)
    
    V(g)$weighted_degree <- weighted_degree # considerando fractional w
    V(g)$degree  <- normal_degree  # considerando aparaciones como peso
    V(g)$degree2 <- normal_degree2 # sin peso

    # Normalizar algunas métricas para visualización
    n_vertices <- vcount(g)
    
    V(g)$betweenness_norm <- betweenness(g, normalized = TRUE)
    V(g)$closeness_norm <- igraph::closeness(g, normalized = TRUE)
      
    # Detección de comunidades
    communities <- cluster_infomap(g)
    V(g)$community <- membership(communities)
    
    # Centralidades adicionales
    V(g)$closeness <- closeness(g)
    V(g)$eigen <- eigen_centrality(g)$vector
    V(g)$pagerank <- page_rank(g)$vector
    
    # Métricas de estructura local
    V(g)$clustering <- transitivity(g, type="local", vids=V(g))
    V(g)$eccentricity <- eccentricity(g)
    
    # Métricas para redes dirigidas
    V(g)$in_degree <- degree(g, mode="in")
    V(g)$out_degree <- degree(g, mode="out")
    V(g)$hub <- hub_score(g)$vector
    V(g)$authority <- authority_score(g)$vector
    
    # Métricas de vecindario
    V(g)$neighborhood_1 <- ego_size(g, 1)
    V(g)$neighborhood_2 <- ego_size(g, 2)

    # auxiliares --------------------------------------------------------------
    g_vertex <-  as_tibble(igraph::as_data_frame(g, what = "vertices"))
    
    g_metrics <- get_network_metrics(g)
    g_metrics
    
    # export ------------------------------------------------------------------
    cli::cli_inform(fout)
    
    saveRDS(
      list(g = g, vertices = g_vertex, metrics = g_metrics),
      fout
    )
    
  })
