# R/batchdockets.R

#' New BatchDockets query
#' @export
bd_new <- function() {
  structure(list(
    search       = NULL,
    filter_parts = character(0),
    select       = c("ResultId"),
    top          = 50L,
    skip         = 0L,
    count        = TRUE
  ), class = "bd_query")
}

#' @export
bd_search <- function(q, term) { q$search <- term; q }

#' @export
bd_filter_date <- function(q, start, end, field = "Date") {
  stopifnot(nchar(start)==10, nchar(end)==10)
  start_dt <- paste0(start, "T00:00:00Z")
  end_dt   <- paste0(end,   "T23:59:59Z")
  q$filter_parts <- c(q$filter_parts,
                      sprintf("%s ge %s and %s le %s", field, start_dt, field, end_dt))
  q
}

bd_filter_eq <- function(q, field, values) {
  values <- unique(na.omit(values)); if (!length(values)) return(q)
  clause <- paste0("(", paste0(field, " eq ", "'", gsub("'", "''", values, fixed = TRUE), "'", collapse = " or "), ")")
  q$filter_parts <- c(q$filter_parts, clause); q
}

#' @export
bd_filter_court <- function(q, v) bd_filter_eq(q, "Court", v)

#' @export
bd_top <- function(q, n) { q$top <- as.integer(min(n, 50L)); q }

bd_build_query <- function(q) {
  list(
    `$search` = q$search,
    `$select` = if (length(q$select)) paste(q$select, collapse = ",") else NULL,
    `$filter` = if (length(q$filter_parts)) paste(q$filter_parts, collapse = " and ") else NULL,
    `$top`    = q$top %||% 50L,
    `$count`  = "true",
    `$skip`   = if (q$skip > 0L) q$skip else NULL
  )
}

#' @export
bd_debug_url <- function(q, endpoint = "/BatchDockets") ln_build_url(endpoint, bd_build_query(q))

#' Plain BatchDockets fetch (IDs only)
#' @export
bd_fetch <- function(q,
                     max_n       = Inf,
                     page_sleep  = 0,
                     sleep_after = 0,
                     endpoint    = "/BatchDockets",
                     retry       = 3,
                     retry_wait  = 1,
                     verbose     = TRUE) {
  stopifnot(inherits(q, "bd_query"))
  page_size <- q$top; if (is.null(page_size) || !is.finite(page_size)) page_size <- 50L
  if (page_size > 50L) { warning("BatchDockets $top max is 50; using 50"); page_size <- 50L }
  all_rows <- list(); page <- 1L; total <- NA_integer_; fetched <- 0L

  obj <- ln_get_json(endpoint, query = bd_build_query(q),
                     retry = retry, retry_wait = retry_wait, sleep_after = sleep_after,
                     ua = "lexisbatch/BatchDockets")
  rows <- if (is.null(obj$value)) tibble::tibble() else tibble::as_tibble(obj$value)
  if (is.na(total)) {
    total <- obj[["@odata.count"]] %||% obj[["odata.count"]] %||%
      obj[["'@odata.count'"]] %||% obj[["'odata.count'"]] %||% NA_integer_
  }
  n_this <- nrow(rows); fetched <- fetched + n_this
  if (verbose) message(sprintf("  | Page %d | +%d, cum=%s/%s", page, n_this,
                               format(fetched, big.mark=","), ifelse(is.na(total), "?", format(total, big.mark=","))))
  if (n_this) all_rows[[length(all_rows)+1L]] <- rows

  next_url <- obj[["@odata.nextLink"]] %||% NULL
  while (!is.null(next_url) && fetched < max_n) {
    obj  <- ln_get_json(next_url, retry = retry, retry_wait = retry_wait, sleep_after = sleep_after,
                        ua = "lexisbatch/BatchDockets")
    rows <- if (is.null(obj$value)) tibble::tibble() else tibble::as_tibble(obj$value)
    n_this <- nrow(rows); fetched <- fetched + n_this; page <- page + 1L
    if (verbose) message(sprintf("  | Page %d | +%d, cum=%s/%s", page, n_this,
                                 format(fetched, big.mark=","), ifelse(is.na(total), "?", format(total, big.mark=","))))
    if (n_this) all_rows[[length(all_rows)+1L]] <- rows
    next_url <- obj[["@odata.nextLink"]] %||% NULL
    if (page_sleep > 0) Sys.sleep(page_sleep)
  }

  out <- dplyr::bind_rows(all_rows)
  attr(out, "total_reported") <- total
  attr(out, "per_page")       <- page_size
  attr(out, "fetched")        <- nrow(out)
  out
}

# build doc URL from ResultId
bd_resultid_to_media_url <- function(result_id) {
  doc_path <- paste0("/shared/document/dockets/", result_id)
  encoded  <- utils::URLencode(doc_path, reserved = TRUE)
  paste0("Documents(DocumentId='", encoded, "',DocumentIdType='DocFullPath')/$value?ind=n")
}

# fetch XML for IDs
bd_collect_xml_from_ids <- function(result_ids,
                                    parallel = FALSE,
                                    workers = max(1, parallel::detectCores()-1),
                                    page_sleep = 0) {
  if (!length(result_ids)) return(tibble::tibble(ResultId = character(), xml = character()))
  fetch_one <- function(rid) {
    url <- bd_resultid_to_media_url(rid)
    doc <- tryCatch(ln_get_xml(url, ua = "lexisbatch/BatchDockets"), error = function(e) NULL)
    if (is.null(doc)) return(tibble::tibble(ResultId = rid, xml = NA_character_))
    tibble::tibble(ResultId = rid, xml = as.character(doc))
  }
  res <- purrr::map(result_ids, fetch_one)
  if (page_sleep > 0) Sys.sleep(page_sleep)
  dplyr::bind_rows(res)
}

# parse docket XML (same as earlier — shortened a bit)
txt1d  <- function(node, xp) { n <- xml2::xml_find_first(node, xp); if (inherits(n,"xml_node")) xml2::xml_text(n, trim=TRUE) else NULL }

parse_docket_xml_rich <- function(xml_str) {
  if (!nzchar(xml_str)) return(NULL)
  doc <- tryCatch(xml2::read_xml(xml_str), error = function(e) NULL)
  if (is.null(doc)) return(NULL)
  entry <- xml2::xml_find_first(doc, "/*[local-name()='entry']")
  if (!inherits(entry, "xml_node")) return(NULL)
  atom_id  <- txt1d(entry, "./*[local-name()='id']")
  atom_ttl <- txt1d(entry, "./*[local-name()='title']")
  list(atom = list(id = atom_id, title = atom_ttl), raw = xml_str)
}

#' turn docket rows with xml into id-keyed JSON
#' @export
dockets_to_idkeyed_json <- function(df, id_col = "ResultId", xml_col = "xml",
                                    pretty = TRUE, auto_unbox = TRUE) {
  ids  <- df[[id_col]]
  xmls <- df[[xml_col]]
  parsed <- purrr::map(xmls, parse_docket_xml_rich)
  out_list <- list()
  for (i in seq_along(ids)) {
    rid <- ids[[i]]
    pj  <- parsed[[i]]
    if (is.null(pj)) next
    if (is.null(pj$atom$id) || !nzchar(pj$atom$id %||% "")) pj$atom$id <- rid
    out_list[[rid]] <- pj
  }
  jsonlite::toJSON(out_list, auto_unbox = auto_unbox, pretty = pretty, null = "null")
}

#' Resumable fetch for BatchDockets (IDs → XML → JSON)
#' Saves into BatchDockets/<search>/
#' @export
bd_run_full_resumable <- function(q,
                                  page_sleep   = 0,
                                  sleep_after  = 0,
                                  endpoint     = "/BatchDockets",
                                  retry        = 3,
                                  retry_wait   = 1,
                                  verbose      = TRUE,
                                  parallel_xml = FALSE,
                                  workers      = max(1, parallel::detectCores()-1),
                                  base_dir     = "BatchDockets",
                                  write_json   = TRUE) {
  if (!nzchar(Sys.getenv("LN_TOKEN"))) lexis_auth_env()

  safe_search <- make_safe_name(q$search)
  out_dir     <- file.path(base_dir, safe_search)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  checkpoint_path <- file.path(out_dir, "checkpoint.rds")
  cp <- safe_read_rds(checkpoint_path)
  if (!is.null(cp)) {
    seen_ids <- cp$seen_ids %||% character(0)
    total    <- cp$total    %||% NA_integer_
    if (verbose) message("Resuming dockets with ", length(seen_ids), " seen IDs.")
  } else {
    seen_ids <- character(0)
    total    <- NA_integer_
  }

  message("[DEBUG URL] ", bd_debug_url(q, endpoint))

  obj <- ln_get_json(endpoint, query = bd_build_query(q),
                     retry = retry, retry_wait = retry_wait, sleep_after = sleep_after,
                     ua = "lexisbatch/BatchDockets")

  all_new_ids <- list()
  page <- 1L

  rows <- if (is.null(obj$value)) tibble::tibble() else tibble::as_tibble(obj$value)
  if (is.na(total)) {
    total <- obj[["@odata.count"]] %||% obj[["odata.count"]] %||%
      obj[["'@odata.count'"]] %||% obj[["'odata.count'"]] %||% NA_integer_
  }

  if (nrow(rows) && "ResultId" %in% names(rows)) {
    new_rows <- rows[!(rows$ResultId %in% seen_ids), , drop = FALSE]
  } else new_rows <- rows

  if (nrow(new_rows)) {
    all_new_ids[[length(all_new_ids)+1L]] <- new_rows
    ids_pages <- list.files(out_dir, pattern = "^ids_page_\\d+\\.rds$")
    next_idx  <- length(ids_pages) + 1L
    saveRDS(new_rows, file.path(out_dir, sprintf("ids_page_%03d.rds", next_idx)))
  }

  if (verbose) {
    message(sprintf("  | IDs page %d | raw=%d, new=%d, seen=%d%s",
                    page, nrow(rows), nrow(new_rows),
                    length(seen_ids) + nrow(new_rows),
                    if (!is.na(total)) paste0("/", total) else ""))
  }

  if (nrow(new_rows)) seen_ids <- c(seen_ids, new_rows$ResultId)

  # save checkpoint after first page
  cp <- list(
    seen_ids = seen_ids,
    total    = total,
    query    = q,
    endpoint = endpoint
  )
  saveRDS(cp, checkpoint_path)

  next_url <- obj[["@odata.nextLink"]] %||% NULL
  while (!is.null(next_url)) {
    obj  <- ln_get_json(next_url, retry = retry, retry_wait = retry_wait, sleep_after = sleep_after,
                        ua = "lexisbatch/BatchDockets")
    rows <- if (is.null(obj$value)) tibble::tibble() else tibble::as_tibble(obj$value)
    page <- page + 1L

    if (nrow(rows) && "ResultId" %in% names(rows)) {
      new_rows <- rows[!(rows$ResultId %in% seen_ids), , drop = FALSE]
    } else new_rows <- rows

    if (nrow(new_rows)) {
      all_new_ids[[length(all_new_ids)+1L]] <- new_rows
      ids_pages <- list.files(out_dir, pattern = "^ids_page_\\d+\\.rds$")
      next_idx  <- length(ids_pages) + 1L
      saveRDS(new_rows, file.path(out_dir, sprintf("ids_page_%03d.rds", next_idx)))
    }

    if (verbose) {
      message(sprintf("  | IDs page %d | raw=%d, new=%d, seen=%d%s",
                      page, nrow(rows), nrow(new_rows),
                      length(seen_ids) + nrow(new_rows),
                      if (!is.na(total)) paste0("/", total) else ""))
    }

    if (nrow(new_rows)) seen_ids <- c(seen_ids, new_rows$ResultId)

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

  new_id_rows <- dplyr::bind_rows(all_new_ids)
  if (!nrow(new_id_rows)) {
    if (verbose) message("No new IDs to fetch XML for.")
    return(new_id_rows)
  }

  if (verbose) message("== Fetching XML for ", nrow(new_id_rows), " new dockets ...")
  xml_rows <- bd_collect_xml_from_ids(new_id_rows$ResultId,
                                      parallel = parallel_xml,
                                      workers = workers,
                                      page_sleep = page_sleep)

  xml_pages <- list.files(out_dir, pattern = "^xml_page_\\d+\\.rds$")
  next_xml  <- length(xml_pages) + 1L
  saveRDS(xml_rows, file.path(out_dir, sprintf("xml_page_%03d.rds", next_xml)))

  if (verbose) message("== Parsing XML → JSON ...")
  json_blob <- dockets_to_idkeyed_json(xml_rows, id_col = "ResultId", xml_col = "xml")
  attr(xml_rows, "parsed_json") <- json_blob

  if (write_json) {
    writeLines(json_blob, file.path(out_dir, "BatchDockets_id_keyed.json"))
  }

  xml_rows
}
