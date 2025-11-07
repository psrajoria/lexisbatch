# R/batchnews.R

#' New BatchNews query
#' @export
bn_new <- function() {
  structure(list(
    search       = NULL,
    filter_parts = character(0),
    select       = c("ResultId","Title","Date"),
    expand       = character(0),
    top          = 50L,
    skip         = 0L,
    count        = TRUE
  ), class = "bn_query")
}

#' Set search expression for BatchNews
#' @export
bn_search <- function(q, term) { q$search <- term; q }

bn_filter_eq <- function(q, field, values) {
  values <- unique(na.omit(values))
  if (!length(values)) return(q)
  clause <- paste0("(", paste0(field, " eq ", "'", gsub("'", "''", values, fixed = TRUE), "'", collapse = " or "), ")")
  q$filter_parts <- c(q$filter_parts, clause)
  q
}

#' Date filter for BatchNews (yyyy-mm-dd)
#' @export
bn_filter_date <- function(q, start, end) {
  stopifnot(nchar(start) == 10, nchar(end) == 10)
  q$filter_parts <- c(q$filter_parts,
                      sprintf("Date ge %s and Date le %s", start, end))
  q
}

#' Filter by publisher name
#' @export
bn_filter_publisher <- function(q, publisher_names) bn_filter_eq(q, "Publisher", publisher_names)

#' Expand related entities
#' @export
bn_expand <- function(q, document = FALSE, source = FALSE) {
  if (document) q$expand <- c(q$expand, "Document")
  if (source)   q$expand <- c(q$expand, "Source($select=Name,Id)")
  q
}

#' Limit per-page (max 50)
#' @export
bn_top  <- function(q, n) { q$top  <- as.integer(min(n, 50L)); q }

# build query list
bn_build_query <- function(q) {
  stopifnot(inherits(q, "bn_query"))
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

#' Build / inspect the URL for debugging
#' @export
bn_debug_url <- function(q, endpoint = "/BatchNews") {
  ln_build_url(endpoint, bn_build_query(q))
}

#' Plain in-memory BatchNews fetch (original)
#' @export
bn_fetch <- function(q,
                     max_n       = Inf,
                     page_sleep  = 0,
                     sleep_after = 0,
                     endpoint    = "/BatchNews",
                     retry       = 2,
                     retry_wait  = 1,
                     verbose     = TRUE) {
  stopifnot(inherits(q, "bn_query"))
  page_size <- q$top
  if (is.null(page_size) || !is.finite(page_size)) page_size <- 50L
  if (page_size > 50L) {
    warning("BatchNews $top max is 50; using 50")
    page_size <- 50L
  }

  all_rows <- list()
  page     <- 1L
  total    <- NA_integer_
  fetched  <- 0L
  skip     <- if (is.null(q$skip)) 0L else as.integer(q$skip)

  repeat {
    qry <- bn_build_query(q)
    if (skip > 0L) qry$`$skip` <- skip

    obj <- ln_get_json(endpoint, query = qry, retry = retry, retry_wait = retry_wait,
                       sleep_after = sleep_after, ua = "lexisbatch/BatchNews")

    val  <- obj$value
    rows <- if (is.null(val)) tibble::tibble() else tibble::as_tibble(val)

    if (is.na(total)) {
      total <- obj[["@odata.count"]] %||% obj[["odata.count"]] %||%
        obj[["'@odata.count'"]] %||% obj[["'odata.count'"]] %||% NA_integer_
    }

    n_this <- nrow(rows)
    fetched <- fetched + n_this

    if (verbose) {
      message(sprintf("  | Page %d | +%d, cum=%s/%s", page, n_this,
                      format(fetched, big.mark=","), ifelse(is.na(total), "?", format(total, big.mark=","))))
    }

    if (n_this) all_rows[[length(all_rows)+1L]] <- rows

    if (n_this < page_size) break
    if (fetched >= max_n) break

    skip <- skip + page_size
    page <- page + 1L
    if (page_sleep > 0) Sys.sleep(page_sleep)
  }

  out <- dplyr::bind_rows(all_rows)
  attr(out, "total_reported") <- total
  attr(out, "per_page")       <- page_size
  attr(out, "fetched")        <- nrow(out)
  out
}

#' Resumable / checkpointed BatchNews fetch
#' Saves to BatchNews/<search>/...
#' @export
bn_fetch_resumable_auto <- function(q,
                                    max_n       = Inf,
                                    page_sleep  = 0,
                                    sleep_after = 0,
                                    endpoint    = "/BatchNews",
                                    retry       = 2,
                                    retry_wait  = 1,
                                    verbose     = TRUE,
                                    base_dir    = "BatchNews") {
  stopifnot(inherits(q, "bn_query"))

  safe_search <- make_safe_name(q$search)
  out_dir     <- file.path(base_dir, safe_search)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  checkpoint_path <- file.path(out_dir, "checkpoint.rds")

  cp <- safe_read_rds(checkpoint_path)
  if (!is.null(cp)) {
    seen_ids <- cp$seen_ids %||% character(0)
    fetched  <- cp$fetched  %||% 0L
    total    <- cp$total    %||% NA_integer_
    if (verbose) message("Resuming with ", length(seen_ids), " seen ids.")
  } else {
    seen_ids <- character(0); fetched <- 0L; total <- NA_integer_
  }

  page_size <- q$top
  if (is.null(page_size) || !is.finite(page_size)) page_size <- 50L
  if (page_size > 50L) { warning("BatchNews $top max is 50; using 50"); page_size <- 50L }

  skip <- 0L
  page <- 1L
  pages_this_run <- list()

  repeat {
    qry <- bn_build_query(q)
    if (skip > 0L) qry$`$skip` <- skip

    obj <- ln_get_json(endpoint, query = qry, retry = retry, retry_wait = retry_wait,
                       sleep_after = sleep_after, ua = "lexisbatch/BatchNews")
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
      fetched  <- fetched + n_new
      seen_ids <- c(seen_ids, rows$ResultId)
      pages_this_run[[length(pages_this_run)+1L]] <- rows

      existing_pages <- list.files(out_dir, pattern = "^page_\\d+\\.rds$")
      next_idx <- length(existing_pages) + 1L
      saveRDS(rows, file.path(out_dir, sprintf("page_%03d.rds", next_idx)))
    }

    if (verbose) {
      message(sprintf("  | Page %d (raw %d, new %d) | cum new=%s%s",
                      page, n_raw, n_new,
                      format(fetched, big.mark=","),
                      if (!is.na(total)) paste0("/", format(total, big.mark=",")) else ""))
    }

    cp <- list(
      seen_ids = seen_ids,
      fetched  = fetched,
      total    = total,
      query    = q,
      endpoint = endpoint
    )
    saveRDS(cp, checkpoint_path)

    if (n_raw < page_size) break
    if (fetched >= max_n) break

    skip <- skip + page_size
    page <- page + 1L
    if (page_sleep > 0) Sys.sleep(page_sleep)
  }

  out <- dplyr::bind_rows(pages_this_run)
  attr(out, "total_reported") <- total
  attr(out, "per_page")       <- page_size
  attr(out, "fetched")        <- fetched
  out
}
