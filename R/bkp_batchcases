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

# convenience
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

# --- XML helpers for cases ---
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
                           parallel = TRUE,
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

# --- parse case XML (shortened from your big version, but keeps main fields) ---
txt1  <- function(node, xp) { n <- xml2::xml_find_first(node, xp); if (inherits(n,"xml_node")) xml2::xml_text(n, trim=TRUE) else NULL }
txts  <- function(node, xp) { v <- xml2::xml_find_all(node, xp); if (length(v)) xml2::xml_text(v, trim=TRUE) else character(0) }
null_if_empty <- function(x) if (is.null(x)) NULL else if (is.atomic(x) && length(x)==0) NULL else if (is.list(x) && !length(x)) NULL else x

parse_case_xml_rich <- function(xml_str) {
  if (!nzchar(xml_str)) return(NULL)
  doc <- tryCatch(xml2::read_xml(xml_str), error = function(e) NULL)
  if (is.null(doc)) return(NULL)

  entry <- xml2::xml_find_first(doc, "/*[local-name()='entry']")
  if (inherits(entry, "xml_node")) {
    ccd <- xml2::xml_find_first(entry, "./*[local-name()='content']/*[1]")
  } else {
    ccd <- xml2::xml_find_first(doc, "/*[local-name()='courtCaseDoc']")
    if (!inherits(ccd, "xml_node")) return(NULL)
    entry <- doc
  }

  atom_id   <- txt1(entry, "./*[local-name()='id']")
  atom_ttl  <- txt1(entry, "./*[local-name()='title']")

  list(
    atom = list(id = atom_id, title = atom_ttl),
    raw  = xml_str
  )
}

#' Convert rows with xml to id-keyed JSON
#' @export
cases_to_idkeyed_json <- function(df, id_col = "ResultId", xml_col = "xml",
                                  pretty = TRUE, auto_unbox = TRUE) {
  ids  <- df[[id_col]]
  xmls <- df[[xml_col]]
  parsed <- purrr::map(xmls, parse_case_xml_rich)
  out_list <- list()
  for (i in seq_along(ids)) {
    rid <- ids[[i]]
    pj  <- parsed[[i]]
    if (is.null(pj)) next
    out_list[[rid]] <- pj
  }
  jsonlite::toJSON(out_list, auto_unbox = auto_unbox, pretty = pretty, null = "null")
}

#' Resumable / checkpointed BatchCases fetch (IDs/rows only)
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

#' High-level runner like your original: fetch, (optional) xml, (optional) parse
#' @export
bc_run <- function(q,
                   page_sleep  = 0,
                   sleep_after = 0,
                   endpoint    = "/BatchCases",
                   retry       = 3,
                   retry_wait  = 1,
                   verbose     = TRUE,
                   fetch_xml   = TRUE,
                   parse_xml   = TRUE) {
  if (!nzchar(Sys.getenv("LN_TOKEN"))) lexis_auth_env()
  message("[DEBUG URL] ", bc_debug_url(q, endpoint))
  rows <- bc_fetch(q, page_sleep = page_sleep, sleep_after = sleep_after,
                   endpoint = endpoint, retry = retry, retry_wait = retry_wait,
                   verbose = verbose)
  if (fetch_xml) {
    rows <- bc_collect_xml(rows, parallel = FALSE)
  }
  if (parse_xml && "xml" %in% names(rows)) {
    message("== Parsing XML → JSON ...")
    json_blob <- cases_to_idkeyed_json(rows, id_col = "ResultId", xml_col = "xml")
    attr(rows, "parsed_json") <- json_blob
  }
  rows
}
