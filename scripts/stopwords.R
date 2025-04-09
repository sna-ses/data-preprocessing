
roman_numbers <- c("i", "ii", "iii", "iv", "v", "vi", "vi", "viii", "ix", "x")

mesures <- c("kg", "mm", "mg", "gr", "m", "usd", "ng", "g", "kg.", "mm.", "mg.", "gr.", "m.", "ng.", "g.")

months <- c("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")

genre_words <-   c("result", "data", "finding", "paper", "study", "aim", "objective", "goal", "discussion", "purpose", "methodology", "author", "research", "introduction", "analysis", "addition", "outcome", "report", "method", "approach", "article", "literature", "review", "design", "researcher", "data", "datum", "aspect", "point", "other", "authors", "affect")

noise <- c("google", "scholar", "https", "http", "scopus", "doi.org", "full", "text", "pubmed", "pdf", "share", "full", "version", "publication", "date", "special", "issue", "vol.", "press", "pp", "e.g.", "i.e.", "et", "al", "ed", "journal", "book", "chapter", "P.", "p.", "no.@card@", "&#x0", "lb/acre", "university", "a.d.", "pp.", "$.", "f.", "h.", "l.", "n°")

stopwords <- tibble::tibble(stopword = c(genre_words, noise, mesures, roman_numbers, months))


