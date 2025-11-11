# R/batchcases.R

#' New BatchCases query
#' @export
bc_new <- function() {
  structure(list(
    search       = NULL,
    filter_parts = character(0),
    select       = c("ResultId","Title","Date"),
    expand       = character(0),
    top          = 50L,
    skip         = 0L,
    count        = TRUE
  ), class = "bc_query")
}

#' @export
bc_search <- function(q, term) { q$search <- term; q }

#' @export
bc_filter_date <- function(q, start, end) {
  stopifnot(nchar(start)==10, nchar(end)==10)
  start_dt <- paste0(start, "T00:00:00Z")
  end_dt   <- paste0(end,   "T23:59:59Z")
  q$filter_parts <- c(q$filter_parts,
                      sprintf("Date ge %s and Date le %s", start_dt, end_dt))
  q
}

bc_filter_eq <- function(q, field, values) {
  values <- unique(na.omit(values)); if (!length(values)) return(q)
  clause <- paste0("(", paste0(field, " eq ", "'", gsub("'", "''", values, fixed = TRUE), "'", collapse = " or "), ")")
  q$filter_parts <- c(q$filter_parts, clause); q
}

#' @export
bc_filter_source_name <- function(q, v) bc_filter_eq(q, "Source/Name", v)

#' @export
bc_expand <- function(q, document = FALSE, source = FALSE) {
  if (document) q$expand <- c(q$expand, "Document")
  if (source)   q$expand <- c(q$expand, "Source($select=Id,Name)")
  q
}

#' @export
bc_select <- function(q, ...) { q$select <- unique(c(...)); q }

#' @export
bc_top  <- function(q, n) { q$top  <- as.integer(min(n, 50L)); q }

bc_build_query <- function(q) {
  list(
    `$search` = q$search,
    `$select` = if (length(q$select)) paste(q$select, collapse = ",") else NULL,
    `$filter` = if (length(q$filter_parts)) paste(q$filter_parts, collapse = " and ") else NULL,
    `$expand` = if (length(q$expand)) paste(q$expand, collapse = ",") else NULL,
    `$top`    = q$top %||% 50L,
    `$count`  = "true",
    `$skip`   = if (q$skip > 0L) q$skip else NULL
  )
}

#' @export
bc_debug_url <- function(q, endpoint = "/BatchCases") ln_build_url(endpoint, bc_build_query(q))

# ------------------------------------------------------------
# plain fetch (no checkpoint) – unchanged
# ------------------------------------------------------------
#' Plain BatchCases fetch following @odata.nextLink
#' @export
bc_fetch <- function(q,
                     page_sleep  = 0,
                     sleep_after = 0,
                     endpoint    = "/BatchCases",
                     retry       = 3,
                     retry_wait  = 1,
                     verbose     = TRUE) {
  stopifnot(inherits(q, "bc_query"))
  all_rows <- list(); page <- 1L; total <- NA_integer_; fetched <- 0L

  obj <- ln_get_json(endpoint, query = bc_build_query(q),
                     retry = retry, retry_wait = retry_wait, sleep_after = sleep_after,
                     ua = "lexisbatch/BatchCases")
  val <- obj$value
  rows <- if (is.null(val)) tibble::tibble() else tibble::as_tibble(val)

  if (is.na(total)) {
    total <- obj[["@odata.count"]] %||% obj[["odata.count"]] %||%
      obj[["'@odata.count'"]] %||% obj[["'odata.count'"]] %||% NA_integer_
  }

  n_this <- nrow(rows); fetched <- fetched + n_this
  if (verbose) message(sprintf("  | Page %d | +%d, cum=%s/%s", page, n_this,
                               format(fetched, big.mark=","), ifelse(is.na(total), "?", format(total, big.mark=","))))
  if (n_this) all_rows[[length(all_rows)+1L]] <- rows

  next_url <- obj[["@odata.nextLink"]] %||% NULL
  while (!is.null(next_url)) {
    obj  <- ln_get_json(next_url, retry = retry, retry_wait = retry_wait, sleep_after = sleep_after,
                        ua = "lexisbatch/BatchCases")
    val  <- obj$value
    rows <- if (is.null(val)) tibble::tibble() else tibble::as_tibble(val)

    n_this <- nrow(rows); fetched <- fetched + n_this; page <- page + 1L
    if (verbose) message(sprintf("  | Page %d | +%d, cum=%s/%s", page, n_this,
                                 format(fetched, big.mark=","), ifelse(is.na(total), "?", format(total, big.mark=","))))
    if (n_this) all_rows[[length(all_rows)+1L]] <- rows

    next_url <- obj[["@odata.nextLink"]] %||% NULL
    if (page_sleep > 0) Sys.sleep(page_sleep)
  }

  out <- dplyr::bind_rows(all_rows)
  attr(out, "total_reported") <- total
  attr(out, "fetched")        <- nrow(out)
  out
}

# ------------------------------------------------------------
# XML helpers
# ------------------------------------------------------------
bc_find_inline_xml_col <- function(df) {
  cand <- grep("^(Document(\\.|$)|DocumentContent(\\.|$)).*(content$|^content$)|^DocumentContent$", names(df),
               value = TRUE, ignore.case = TRUE)
  if (length(cand)) cand[1] else NA_character_
}

bc_media_link_col <- function(df) {
  cand <- grep("@odata\\.mediaReadLink$", names(df), value = TRUE)
  if (length(cand)) return(cand[1])
  cand <- grep("mediaReadLink", names(df), value = TRUE, ignore.case = TRUE)
  if (length(cand)) return(cand[1])
  NA_character_
}

#' Fetch XML for BatchCases rows
#' @export
bc_collect_xml <- function(rows,
                           parallel = FALSE,
                           workers = max(1, parallel::detectCores()-1),
                           page_sleep = 0) {
  inline_col <- bc_find_inline_xml_col(rows)
  if (!is.na(inline_col)) {
    message("== Using inline XML from column: ", inline_col)
    rows$xml <- rows[[inline_col]]
    return(rows)
  }

  mcol <- bc_media_link_col(rows)
  if (is.na(mcol)) {
    warning("No inline Document content and no @odata.mediaReadLink column present. Did you include bc_expand(document = TRUE)?")
    rows$xml <- NA_character_
    return(rows)
  }

  urls <- rows[[mcol]]
  ids  <- rows$ResultId

  message("== Downloading Atom XML via @odata.mediaReadLink for ", length(urls), " rows ...")
  fetch_one <- function(u, id) {
    doc <- tryCatch(ln_get_xml(u, ua = "lexisbatch/BatchCases"), error = function(e) NULL)
    if (is.null(doc)) return(tibble::tibble(ResultId = id, xml = NA_character_))
    tibble::tibble(ResultId = id, xml = as.character(doc))
  }

  res <- purrr::map2(urls, ids, fetch_one)
  if (page_sleep > 0) Sys.sleep(page_sleep)
  dplyr::left_join(rows, dplyr::bind_rows(res), by = "ResultId")
}

# ------------------------------------------------------------
# RICH XML PARSER (the big one)
# ------------------------------------------------------------
nz_or <- function(x, alt = NULL) if (!is.null(x) && length(x) && all(nchar(x) > 0)) x else alt
txt1  <- function(node, xp) { n <- xml2::xml_find_first(node, xp); if (inherits(n, "xml_node")) xml2::xml_text(n, trim=TRUE) else NULL }
txts  <- function(node, xp) { v <- xml2::xml_find_all(node, xp); if (length(v)) xml2::xml_text(v, trim=TRUE) else character(0) }
attr1 <- function(node, xp, attr) { n <- xml2::xml_find_first(node, xp); if (inherits(n, "xml_node")) xml2::xml_attr(n, attr) else NULL }
attrs <- function(node, xp, attr) { v <- xml2::xml_find_all(node, xp); if (length(v)) xml2::xml_attr(v, attr) else character(0) }
null_if_empty <- function(x) if (is.null(x)) NULL else if (is.atomic(x) && length(x)==0) NULL else if (is.list(x) && !length(x)) NULL else x

parse_case_xml_rich <- function(xml_str) {
  if (!nzchar(xml_str)) return(NULL)
  doc <- tryCatch(xml2::read_xml(xml_str), error = function(e) NULL)
  if (is.null(doc)) return(NULL)

  # Atom-wrapped or bare courtCaseDoc
  entry <- xml2::xml_find_first(doc, "/*[local-name()='entry']")
  if (inherits(entry, "xml_node")) {
    ccd <- xml2::xml_find_first(entry, "./*[local-name()='content']/*[1]")
  } else {
    ccd <- xml2::xml_find_first(doc, "/*[local-name()='courtCaseDoc']")
    if (!inherits(ccd, "xml_node")) return(NULL)
    entry <- doc
  }

  # Atom part
  atom_id   <- txt1(entry, "./*[local-name()='id']")
  atom_ttl  <- txt1(entry, "./*[local-name()='title']")
  atom_pub  <- txt1(entry, "./*[local-name()='published']")
  atom_upd  <- txt1(entry, "./*[local-name()='updated']")

  # dc-ish identifiers
  dc_src  <- txt1(entry, ".//*[local-name()='source']")
  dc_date <- txt1(entry, ".//*[local-name()='date']")
  dc_identifiers <- txts(entry, ".//*[local-name()='identifier']")
  pguid <- NULL; lni <- NULL
  if (length(dc_identifiers)) {
    pguid <- nz_or(dc_identifiers[grepl("^PGUID:", dc_identifiers)], NULL)
    lni   <- nz_or(dc_identifiers[grepl("^LNI:",   dc_identifiers)], NULL)
    if (length(pguid)) pguid <- sub("^PGUID:\\s*", "", pguid[1])
    if (length(lni))   lni   <- sub("^LNI:\\s*",   "", lni[1])
  }

  # case info
  case_name_node <- xml2::xml_find_first(ccd, ".//*[local-name()='caseInfo']/*[local-name()='caseName']")
  full_names <- txts(case_name_node, "./*[local-name()='fullCaseName']")
  short_name <- txt1(case_name_node, "./*[local-name()='shortCaseName']")
  dockets    <- txts(ccd, ".//*[local-name()='docketNumber']")

  court_name <- txt1(ccd, ".//*[local-name()='courtInfo']/*[local-name()='courtName']")
  juris_name <- txt1(ccd, ".//*[local-name()='courtInfo']//*[local-name()='jurisSystem']")
  juris_norm <- attr1(ccd, ".//*[local-name()='courtInfo']//*[local-name()='jurisSystem']", "normalized")

  # dates
  decision_date_norm <- attr1(ccd, ".//*[local-name()='decisionDate']", "normalizedDate")
  decision_date_disp <- txt1(ccd,   ".//*[local-name()='decisionDate']")
  argued_date_norm   <- attr1(ccd, ".//*[local-name()='arguedDate']", "normalizedDate")
  argued_date_disp   <- txt1(ccd,   ".//*[local-name()='arguedDate']")

  # citations
  cite_nodes <- xml2::xml_find_all(ccd, ".//*[local-name()='citeForThisResource']")
  citations <- purrr::map(cite_nodes, function(n){
    list(
      scheme = xml2::xml_attr(n, "pageScheme"),
      key    = xml2::xml_attr(n, "citeDefinition"),
      text   = xml2::xml_text(n, trim = TRUE)
    )
  })

  # history
  hist_nodes <- xml2::xml_find_all(ccd, ".//*[local-name()='caseHistory']//*[local-name()='citation']")
  history <- purrr::map(hist_nodes, function(n){
    list(
      type = xml2::xml_attr(n, "type"),
      text = xml2::xml_text(n, trim = TRUE)
    )
  })

  # judges (dedup)
  judge_nodes <- xml2::xml_find_all(ccd, ".//*[local-name()='panel']//*[local-name()='judge']")
  judges <- purrr::map(judge_nodes, function(n){
    nm <- txt1(n, ".//*[local-name()='nameText' or local-name()='name' or local-name()='fullName']") %||%
      txt1(n, "./*[local-name()='person']/*[local-name()='nameText']") %||%
      xml2::xml_text(n, trim = TRUE)
    nm <- trimws(nm)
    role <- xml2::xml_attr(n, "role") %||% txt1(n, ".//*[local-name()='role']")
    list(name = if (nzchar(nm)) nm else NULL, role = role)
  })
  judges <- purrr::compact(judges)
  if (length(judges)) {
    dfj <- tibble::as_tibble(do.call(rbind, lapply(judges, as.data.frame)))
    dfj <- dfj %>% dplyr::distinct(name, .keep_all = TRUE)
    judges <- split(dfj, seq_len(nrow(dfj))) %>% lapply(as.list)
  } else judges <- NULL

  # counsel
  counsel_nodes <- xml2::xml_find_all(ccd, ".//*[local-name()='representation']//*[local-name()='counselor']")
  counsel <- purrr::map(counsel_nodes, function(n){
    list(
      name = xml2::xml_text(n, trim=TRUE),
      side = xml2::xml_attr(n, "side")
    )
  })

  # opinions
  opinion_nodes <- xml2::xml_find_all(ccd, ".//*[local-name()='caseOpinions']/*[local-name()='opinion']")
  opinions <- purrr::map(opinion_nodes, function(op){
    op_type <- xml2::xml_attr(op, "type")
    op_by   <- txt1(op, "./*[local-name()='caseOpinionBy']")
    p_nodes <- xml2::xml_find_all(op, ".//*[local-name()='bodyText']/*[local-name()='p']")
    paragraphs <- purrr::map(seq_along(p_nodes), function(i){
      p <- p_nodes[[i]]
      list(
        seq    = i,
        id     = xml2::xml_attr(p, "anchor"),
        text   = xml2::xml_text(p, trim=TRUE),
        pages  = purrr::map(xml2::xml_find_all(p, ".//*[local-name()='page']"), function(pg) {
          list(
            scheme = xml2::xml_attr(pg, "paginationScheme"),
            num    = xml2::xml_attr(pg, "number")
          )
        }),
        cites  = xml2::xml_text(xml2::xml_find_all(p, ".//*[local-name()='citation']"), trim=TRUE)
      )
    })
    fn_nodes <- xml2::xml_find_all(op, ".//*[local-name()='footnote']")
    footnotes <- purrr::map(fn_nodes, function(fn){
      list(
        id    = xml2::xml_attr(fn, "anchor"),
        label = txt1(fn, "./*[local-name()='label']"),
        text  = txt1(fn, "./*[local-name()='bodyText']")
      )
    })
    list(type = op_type, by = op_by, paragraphs = paragraphs, footnotes = footnotes)
  })

  # pagination + classifications
  pg_schemes <- attrs(ccd, ".//*[local-name()='pagination']/*[local-name()='paginationScheme']", "name")
  class_items <- xml2::xml_find_all(ccd, ".//*[local-name()='classificationGroup']//*[local-name()='classItem']")
  classifications <- purrr::map(class_items, function(ci){
    list(code = xml2::xml_attr(ci, "code"), text = xml2::xml_text(ci, trim=TRUE))
  })

  list(
    atom = list(
      id        = atom_id,
      title     = atom_ttl,
      published = atom_pub,
      updated   = atom_upd
    ),
    identifiers = list(
      result_id = atom_id,
      pguid     = pguid,
      lni       = lni,
      source    = dc_src,
      dc_date   = dc_date
    ),
    case = list(
      full_case_name = null_if_empty(full_names),
      short_name     = short_name,
      dockets        = null_if_empty(dockets),
      court          = court_name,
      jurisdiction   = list(name = juris_name, normalized = juris_norm),
      dates = list(
        decision = list(normalized = decision_date_norm, display = decision_date_disp),
        argued   = list(normalized = argued_date_norm,   display = argued_date_disp)
      ),
      citations      = null_if_empty(citations),
      history        = null_if_empty(history),
      judges         = null_if_empty(judges),
      counsel        = null_if_empty(counsel),
      opinions       = null_if_empty(opinions),
      pagination_schemes = null_if_empty(pg_schemes),
      classifications    = null_if_empty(classifications)
    )
  )
}

#' Convert rows with xml to id-keyed JSON
#' @export
cases_to_idkeyed_json <- function(df, id_col = "ResultId", xml_col = "xml",
                                  pretty = TRUE, auto_unbox = TRUE) {
  stopifnot(id_col %in% names(df), xml_col %in% names(df))
  ids  <- df[[id_col]]
  xmls <- df[[xml_col]]
  parsed <- purrr::map(xmls, parse_case_xml_rich)
  out_list <- list()
  for (i in seq_along(ids)) {
    rid <- ids[[i]]
    if (!nzchar(rid)) next
    pj  <- parsed[[i]]
    if (is.null(pj)) next
    # ensure we always have an identifier
    if (is.null(pj$identifiers$result_id) || !nzchar(pj$identifiers$result_id %||% "")) {
      pj$identifiers$result_id <- rid
    }
    out_list[[rid]] <- pj
  }
  jsonlite::toJSON(out_list, auto_unbox = auto_unbox, pretty = pretty, null = "null")
}

# ------------------------------------------------------------
# RESUMABLE FETCH (pages + checkpoint)
# ------------------------------------------------------------
#' Resumable / checkpointed BatchCases fetch (rows only)
#' Saves to BatchCases/<search>/
#' @export
bc_fetch_resumable_auto <- function(q,
                                    page_sleep  = 0,
                                    sleep_after = 0,
                                    endpoint    = "/BatchCases",
                                    retry       = 3,
                                    retry_wait  = 1,
                                    verbose     = TRUE,
                                    base_dir    = "BatchCases") {
  stopifnot(inherits(q, "bc_query"))

  safe_search <- make_safe_name(q$search)
  out_dir     <- file.path(base_dir, safe_search)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  checkpoint_path <- file.path(out_dir, "checkpoint.rds")
  cp <- safe_read_rds(checkpoint_path)
  if (!is.null(cp)) {
    seen_ids <- cp$seen_ids %||% character(0)
    total    <- cp$total    %||% NA_integer_
    if (verbose) message("Resuming BatchCases with ", length(seen_ids), " seen ids.")
  } else {
    seen_ids <- character(0); total <- NA_integer_
  }

  pages_this_run <- list()
  page <- 1L

  obj <- ln_get_json(endpoint, query = bc_build_query(q),
                     retry = retry, retry_wait = retry_wait, sleep_after = sleep_after,
                     ua = "lexisbatch/BatchCases")
  val  <- obj$value
  rows <- if (is.null(val)) tibble::tibble() else tibble::as_tibble(val)

  if (is.na(total)) {
    total <- obj[["@odata.count"]] %||% obj[["odata.count"]] %||%
      obj[["'@odata.count'"]] %||% obj[["'odata.count'"]] %||% NA_integer_
  }

  n_raw <- nrow(rows)
  if (n_raw && "ResultId" %in% names(rows)) {
    rows <- rows[!(rows$ResultId %in% seen_ids), , drop = FALSE]
  }
  n_new <- nrow(rows)

  if (n_new) {
    pages_this_run[[length(pages_this_run)+1L]] <- rows
    seen_ids <- c(seen_ids, rows$ResultId)
    existing_pages <- list.files(out_dir, pattern = "^page_\\d+\\.rds$")
    next_idx <- length(existing_pages) + 1L
    saveRDS(rows, file.path(out_dir, sprintf("page_%03d.rds", next_idx)))
  }

  if (verbose) {
    message(sprintf("  | Page %d (raw %d, new %d) | seen=%d%s",
                    page, n_raw, n_new, length(seen_ids),
                    if (!is.na(total)) paste0("/", total) else ""))
  }

  cp <- list(
    seen_ids = seen_ids,
    total    = total,
    query    = q,
    endpoint = endpoint
  )
  saveRDS(cp, checkpoint_path)

  next_url <- obj[["@odata.nextLink"]] %||% NULL
  while (!is.null(next_url)) {
    obj  <- ln_get_json(next_url,
                        retry = retry, retry_wait = retry_wait, sleep_after = sleep_after,
                        ua = "lexisbatch/BatchCases")
    val  <- obj$value
    rows <- if (is.null(val)) tibble::tibble() else tibble::as_tibble(val)

    page <- page + 1L
    n_raw <- nrow(rows)
    if (n_raw && "ResultId" %in% names(rows)) {
      rows <- rows[!(rows$ResultId %in% seen_ids), , drop = FALSE]
    }
    n_new <- nrow(rows)

    if (n_new) {
      pages_this_run[[length(pages_this_run)+1L]] <- rows
      seen_ids <- c(seen_ids, rows$ResultId)
      existing_pages <- list.files(out_dir, pattern = "^page_\\d+\\.rds$")
      next_idx <- length(existing_pages) + 1L
      saveRDS(rows, file.path(out_dir, sprintf("page_%03d.rds", next_idx)))
    }

    if (verbose) {
      message(sprintf("  | Page %d (raw %d, new %d) | seen=%d%s",
                      page, n_raw, n_new, length(seen_ids),
                      if (!is.na(total)) paste0("/", total) else ""))
    }

    cp <- list(
      seen_ids = seen_ids,
      total    = total,
      query    = q,
      endpoint = endpoint
    )
    saveRDS(cp, checkpoint_path)

    next_url <- obj[["@odata.nextLink"]] %||% NULL
    if (page_sleep > 0) Sys.sleep(page_sleep)
  }

  out <- dplyr::bind_rows(pages_this_run)
  attr(out, "total_reported") <- total
  attr(out, "seen_total")     <- length(seen_ids)
  out
}

# ------------------------------------------------------------
# full run: fetch → merge pages → XML → JSON → save
# ------------------------------------------------------------
#' Resumable BatchCases: fetch → XML → JSON, all saved under BatchCases/<search>/
#' @export
bc_run_resumable_auto <- function(q,
                                  page_sleep   = 0,
                                  sleep_after  = 0,
                                  endpoint     = "/BatchCases",
                                  retry        = 3,
                                  retry_wait   = 1,
                                  verbose      = TRUE,
                                  base_dir     = "BatchCases",
                                  parallel_xml = FALSE,
                                  workers      = max(1, parallel::detectCores()-1)) {

  if (!nzchar(Sys.getenv("LN_TOKEN"))) lexis_auth_env()

  safe_search <- make_safe_name(q$search)
  out_dir     <- file.path(base_dir, safe_search)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  # 1) fetch / resume
  bc_fetch_resumable_auto(
    q,
    page_sleep  = page_sleep,
    sleep_after = sleep_after,
    endpoint    = endpoint,
    retry       = retry,
    retry_wait  = retry_wait,
    verbose     = verbose,
    base_dir    = base_dir
  )

  # 2) load ALL pages we have so far
  page_files <- list.files(out_dir, pattern = "^page_\\d+\\.rds$", full.names = TRUE)
  all_rows <- lapply(page_files, readRDS)
  all_rows <- dplyr::bind_rows(all_rows)

  if (!nrow(all_rows)) {
    message("No rows to fetch XML for.")
    return(all_rows)
  }

  # 3) fetch XML
  all_rows_xml <- bc_collect_xml(all_rows,
                                 parallel   = parallel_xml,
                                 workers    = workers,
                                 page_sleep = page_sleep)

  # 4) parse to JSON
  message("== Parsing XML → JSON ...")
  json_blob <- cases_to_idkeyed_json(all_rows_xml,
                                     id_col = "ResultId",
                                     xml_col = "xml")

  # 5) write JSON inside that folder, named after query
  json_path <- file.path(out_dir, paste0(safe_search, "_id_keyed.json"))
  writeLines(json_blob, json_path)

  attr(all_rows_xml, "parsed_json") <- json_blob
  all_rows_xml
}
