# R/batchdockets.R
# BatchDockets → IDs → Atom XML → rich id-keyed JSON
# with per-query folders + checkpoint, JSON named after the query

# we assume ln_get_json(), ln_get_xml(), ln_build_url(), lexis_auth_env()
# and the helper `%||%` exist in the package (you already have them in another file)

# ----- local helpers (safe) -------------------------------------------------

safe_read_rds <- function(path) {
  if (file.exists(path)) {
    tryCatch(readRDS(path), error = function(e) NULL)
  } else {
    NULL
  }
}

make_safe_name <- function(x) {
  if (is.null(x) || !nzchar(x)) return("no_search")
  x <- gsub("[^A-Za-z0-9._-]+", "_", x)
  substr(x, 1, 80)
}

# ---------------------------------------------------------------------------
# 1) Query builders
# ---------------------------------------------------------------------------

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
  stopifnot(nchar(start) == 10, nchar(end) == 10)
  start_dt <- paste0(start, "T00:00:00Z")
  end_dt   <- paste0(end,   "T23:59:59Z")
  q$filter_parts <- c(
    q$filter_parts,
    sprintf("%s ge %s and %s le %s", field, start_dt, field, end_dt)
  )
  q
}

bd_filter_eq <- function(q, field, values) {
  values <- unique(na.omit(values)); if (!length(values)) return(q)
  clause <- paste0(
    "(",
    paste0(field, " eq ", "'", gsub("'", "''", values, fixed = TRUE), "'", collapse = " or "),
    ")"
  )
  q$filter_parts <- c(q$filter_parts, clause)
  q
}

# some convenience filters (export the ones you need)
#' @export
bd_filter_court <- function(q, v) bd_filter_eq(q, "Court", v)

#' @export
bd_top <- function(q, n) { q$top <- as.integer(min(n, 50L)); q }

bd_build_query <- function(q) {
  stopifnot(inherits(q, "bd_query"))
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
bd_debug_url <- function(q, endpoint = "/BatchDockets") {
  ln_build_url(endpoint, bd_build_query(q))
}

# ---------------------------------------------------------------------------
# 2) Plain fetch (IDs only) — keep original functionality
# ---------------------------------------------------------------------------

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
  if (page_size > 50L) {
    warning("BatchDockets $top max is 50; using 50")
    page_size <- 50L
  }

  all_rows <- list(); page <- 1L; total <- NA_integer_; fetched <- 0L

  obj <- ln_get_json(
    endpoint,
    query       = bd_build_query(q),
    retry       = retry,
    retry_wait  = retry_wait,
    sleep_after = sleep_after,
    ua          = "lexisbatch/BatchDockets"
  )
  rows <- if (is.null(obj$value)) tibble::tibble() else tibble::as_tibble(obj$value)

  if (is.na(total)) {
    total <- obj[["@odata.count"]] %||% obj[["odata.count"]] %||%
      obj[["'@odata.count'"]] %||% obj[["'odata.count'"]] %||% NA_integer_
  }

  n_this <- nrow(rows); fetched <- fetched + n_this
  if (verbose) {
    message(sprintf("  | Page %d | +%d, cum=%s/%s",
                    page, n_this,
                    format(fetched, big.mark = ","),
                    ifelse(is.na(total), "?", format(total, big.mark = ","))))
  }
  if (n_this) all_rows[[length(all_rows)+1L]] <- rows

  next_url <- obj[["@odata.nextLink"]] %||% NULL
  while (!is.null(next_url) && fetched < max_n) {
    obj  <- ln_get_json(
      next_url,
      retry       = retry,
      retry_wait  = retry_wait,
      sleep_after = sleep_after,
      ua          = "lexisbatch/BatchDockets"
    )
    rows <- if (is.null(obj$value)) tibble::tibble() else tibble::as_tibble(obj$value)
    page <- page + 1L

    n_this <- nrow(rows); fetched <- fetched + n_this
    if (verbose) {
      message(sprintf("  | Page %d | +%d, cum=%s/%s",
                      page, n_this,
                      format(fetched, big.mark = ","),
                      ifelse(is.na(total), "?", format(total, big.mark = ","))))
    }
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

# ---------------------------------------------------------------------------
# 3) XML fetch for docket IDs
# ---------------------------------------------------------------------------

# build media URL from ResultId (DocFullPath pattern seen in Dockets)
bd_resultid_to_media_url <- function(result_id) {
  stopifnot(nzchar(result_id))
  doc_path <- paste0("/shared/document/dockets/", result_id)
  encoded  <- utils::URLencode(doc_path, reserved = TRUE)
  paste0("Documents(DocumentId='", encoded, "',DocumentIdType='DocFullPath')/$value?ind=n")
}

bd_collect_xml_from_ids <- function(result_ids,
                                    parallel = FALSE,
                                    workers  = max(1, parallel::detectCores()-1),
                                    page_sleep = 0) {
  if (!length(result_ids)) {
    return(tibble::tibble(ResultId = character(), xml = character()))
  }

  fetch_one <- function(rid) {
    url <- bd_resultid_to_media_url(rid)
    doc <- tryCatch(ln_get_xml(url, ua = "lexisbatch/BatchDockets"), error = function(e) NULL)
    if (is.null(doc)) {
      return(tibble::tibble(ResultId = rid, xml = NA_character_))
    }
    tibble::tibble(ResultId = rid, xml = as.character(doc))
  }

  # we keep it simple (non-parallel) by default; user can turn on parallel if deps are there
  res <- purrr::map(result_ids, fetch_one)

  if (page_sleep > 0) Sys.sleep(page_sleep)
  dplyr::bind_rows(res)
}

# ---------------------------------------------------------------------------
# 4) Rich parser (your long version)
# ---------------------------------------------------------------------------

txt1  <- function(node, xp) { n <- xml2::xml_find_first(node, xp); if (inherits(n,"xml_node")) xml2::xml_text(n, trim=TRUE) else NULL }
txts  <- function(node, xp) { v <- xml2::xml_find_all(node, xp); if (length(v)) xml2::xml_text(v, trim=TRUE) else character(0) }
attr1 <- function(node, xp, attr){ n <- xml2::xml_find_first(node, xp); if (inherits(n,"xml_node")) xml2::xml_attr(n, attr) else NULL }
null_if_empty <- function(x) if (is.null(x)) NULL else if (is.atomic(x) && length(x)==0) NULL else if (is.list(x) && !length(x)) NULL else x

.read_additional_fields <- function(node) {
  addfs <- xml2::xml_find_all(node, ".//*[local-name()='additionalField']")
  if (!length(addfs)) return(NULL)
  out <- list()
  for (n in addfs) {
    k <- txt1(n, "./*[local-name()='fieldName']")
    v <- txt1(n, "./*[local-name()='fieldValue']")
    if (nzchar(k)) out[[k]] <- v
  }
  if (!length(out)) NULL else out
}

.get_add <- function(extras, keys) {
  if (is.null(extras) || !length(extras)) return(NULL)
  lk <- tolower(names(extras))
  for (k in keys) {
    hit <- which(lk == tolower(k))
    if (length(hit)) return(extras[[ hit[1] ]])
  }
  for (k in keys) {
    hit <- which(grepl(tolower(k), lk, fixed = TRUE))
    if (length(hit)) return(extras[[ hit[1] ]])
  }
  NULL
}

parse_docket_xml_rich <- function(xml_str) {
  if (!nzchar(xml_str)) return(NULL)
  doc <- tryCatch(xml2::read_xml(xml_str), error = function(e) NULL)
  if (is.null(doc)) return(NULL)

  entry <- xml2::xml_find_first(doc, "/*[local-name()='entry']")
  if (!inherits(entry, "xml_node")) return(NULL)

  atom_id   <- txt1(entry, "./*[local-name()='id']")
  atom_ttl  <- txt1(entry, "./*[local-name()='title']")
  atom_pub  <- txt1(entry, "./*[local-name()='published']")
  atom_upd  <- txt1(entry, "./*[local-name()='updated']")
  crd       <- xml2::xml_find_first(entry, "./*[local-name()='content']/*[local-name()='caseRelatedDoc']")

  # head / caseInfo
  head   <- xml2::xml_find_first(crd, "./*[local-name()='caseRelatedDocHead']")
  cinfo  <- xml2::xml_find_first(head, "./*[local-name()='caseInfo']")
  cName  <- txt1(head, "./*[local-name()='caseName']")
  court  <- txt1(cinfo, ".//*[local-name()='courtInfo']/*[local-name()='courtName']")
  dnum   <- txt1(cinfo, ".//*[local-name()='identifier' and @idType='docketNumber']")
  filed  <- txt1(cinfo, ".//*[local-name()='date' and @dateType='filed']")
  status <- txt1(cinfo, ".//*[local-name()='classification' and @classificationScheme='caseStatus']/*[local-name()='classificationItem']/*[local-name()='className']")
  status_dt <- txt1(cinfo, ".//*[local-name()='date' and @dateType='statusDate']")
  ctype_code <- txt1(cinfo, ".//*[local-name()='classification' and @classificationScheme='caseType']/*[local-name()='classificationItem']/*[local-name()='classCode']")
  ctype_name <- txt1(cinfo, ".//*[local-name()='classification' and @classificationScheme='caseType']/*[local-name()='classificationItem']/*[local-name()='className']")
  nos_code   <- txt1(cinfo, ".//*[local-name()='classification' and @classificationScheme='caseNos']/*[local-name()='classificationItem']/*[local-name()='classCode']")
  nos_name   <- txt1(cinfo, ".//*[local-name()='classification' and @classificationScheme='caseNos']/*[local-name()='classificationItem']/*[local-name()='className']")

  # extras (global + proceedings)
  extras_all    <- .read_additional_fields(crd)
  evroot        <- xml2::xml_find_first(crd, "./*[local-name()='caseRelatedDocBody']/*[local-name()='docket']/*[local-name()='events']")
  proceed_extra <- .read_additional_fields(evroot)
  extras <- extras_all
  if (is.null(extras)) extras <- list()
  if (!is.null(proceed_extra)) {
    for (nm in names(proceed_extra)) {
      if (is.null(extras[[nm]])) extras[[nm]] <- proceed_extra[[nm]]
    }
  }
  if (!length(extras)) extras <- NULL

  additional_case     <- .get_add(extras, c("Additional Case","Additional Case Information"))
  civil_private       <- .get_add(extras, c("civil - private","Civil - Private"))
  appeal_from         <- .get_add(extras, c("Appeal from"))
  district            <- .get_add(extras, c("District"))
  division            <- .get_add(extras, c("Division"))
  case_number         <- .get_add(extras, c("CaseNumber","Case Number"))
  date_filed_promoted <- .get_add(extras, c("DateFiled","Date Filed")) %||% filed
  trial_judge         <- .get_add(extras, c("Trial Judge","TrialJudge","Judge (Trial)"))
  judgment_date       <- .get_add(extras, c("Judgment Date","Judgement Date"))
  date_noa_filed      <- .get_add(extras, c("Date NOA Filed","Date Notice of Appeal Filed","NOA Filed Date"))

  # participants
  parts       <- xml2::xml_find_first(crd, "./*[local-name()='caseRelatedDocBody']/*[local-name()='docket']/*[local-name()='participants']")
  judge_nodes <- xml2::xml_find_all(parts, ".//*[local-name()='judges']//*[local-name()='judge']")
  judges      <- purrr::map(judge_nodes, \(n) list(name = txt1(n, ".//*[local-name()='nameText']")))
  party_nodes <- xml2::xml_find_all(parts, ".//*[local-name()='parties']/*[local-name()='party']")
  parties     <- purrr::map(party_nodes, \(p) list(
    role = xml2::xml_attr(p, "partyRole"),
    name = txt1(p, ".//*[local-name()='nameText']")
  ))

  # events
  provs  <- xml2::xml_find_all(evroot, "./*[local-name()='proceeding']")
  events <- purrr::map(seq_along(provs), function(i) {
    pr <- provs[[i]]
    list(
      number = xml2::xml_attr(pr, "number"),
      date   = txt1(pr, "./*[local-name()='date' and @dateType='proceedingDate']"),
      text   = txt1(pr, "./*[local-name()='p']"),
      record = txt1(pr, ".//*[local-name()='additionalField'][./*[local-name()='fieldName']='RecordNumber']/*[local-name()='fieldValue']")
    )
  })

  # citations
  cites     <- xml2::xml_find_all(crd, ".//*[local-name()='citations']//*[local-name()='citation']//*[local-name()='span']")
  citations <- purrr::map(cites, \(n) list(
    normalized = xml2::xml_attr(n, "normalizedCite"),
    text       = xml2::xml_text(n, trim = TRUE)
  ))

  list(
    atom = list(
      id       = atom_id,
      title    = atom_ttl,
      published= atom_pub,
      updated  = atom_upd
    ),
    case = list(
      name       = cName,
      docket_no  = dnum,
      court      = court,
      filed      = date_filed_promoted,
      status     = status,
      status_dt  = status_dt,
      case_type  = list(code = ctype_code, name = ctype_name),
      nos        = list(code = nos_code,  name = nos_name),
      extras     = extras,
      promoted   = null_if_empty(list(
        additional_case_info = additional_case %||% civil_private,
        appeal_from          = appeal_from,
        district             = district,
        division             = division,
        case_number_alt      = case_number,
        date_filed_alt       = date_filed_promoted,
        trial_judge          = trial_judge,
        judgment_date        = judgment_date,
        date_noa_filed       = date_noa_filed
      ))
    ),
    participants = list(
      judges  = null_if_empty(judges),
      parties = null_if_empty(parties)
    ),
    events    = null_if_empty(events),
    citations = null_if_empty(citations)
  )
}

#' turn docket rows with xml into id-keyed JSON
#' @export
dockets_to_idkeyed_json <- function(df,
                                    id_col    = "ResultId",
                                    xml_col   = "xml",
                                    pretty    = TRUE,
                                    auto_unbox = TRUE) {
  stopifnot(id_col %in% names(df), xml_col %in% names(df))
  ids  <- df[[id_col]]
  xmls <- df[[xml_col]]
  parsed <- purrr::map(xmls, parse_docket_xml_rich)

  out_list <- list()
  for (i in seq_along(ids)) {
    rid <- ids[[i]]
    if (!nzchar(rid)) next
    pj  <- parsed[[i]]
    if (is.null(pj)) next
    # ensure there's an id
    if (is.null(pj$atom$id) || !nzchar(pj$atom$id %||% "")) {
      pj$atom$id <- rid
    }
    out_list[[rid]] <- pj
  }

  jsonlite::toJSON(out_list, auto_unbox = auto_unbox, pretty = pretty, null = "null")
}

# ---------------------------------------------------------------------------
# 5) Resumable, per-query, with JSON named like query
# ---------------------------------------------------------------------------

#' Resumable BatchDockets: IDs → XML → JSON
#' Saves to BatchDockets/<search>/
#' JSON file will be <search>_id_keyed.json
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
                                  base_dir     = "BatchDockets") {

  # make sure we are authenticated
  if (!nzchar(Sys.getenv("LN_TOKEN"))) lexis_auth_env()

  safe_search <- make_safe_name(q$search)
  out_dir     <- file.path(base_dir, safe_search)
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  checkpoint_path <- file.path(out_dir, "checkpoint.rds")

  # load checkpoint
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

  # ----- first page (always from base query) -----
  obj <- ln_get_json(
    endpoint,
    query       = bd_build_query(q),
    retry       = retry,
    retry_wait  = retry_wait,
    sleep_after = sleep_after,
    ua          = "lexisbatch/BatchDockets"
  )

  all_new_ids <- list()
  page <- 1L
  rows <- if (is.null(obj$value)) tibble::tibble() else tibble::as_tibble(obj$value)

  if (is.na(total)) {
    total <- obj[["@odata.count"]] %||% obj[["odata.count"]] %||%
      obj[["'@odata.count'"]] %||% obj[["'odata.count'"]] %||% NA_integer_
  }

  # drop IDs we've seen
  if (nrow(rows) && "ResultId" %in% names(rows)) {
    new_rows <- rows[!(rows$ResultId %in% seen_ids), , drop = FALSE]
  } else {
    new_rows <- rows
  }

  if (nrow(new_rows)) {
    all_new_ids[[length(all_new_ids)+1L]] <- new_rows
    # save ids page
    ids_pages <- list.files(out_dir, pattern = "^ids_page_\\d+\\.rds$")
    next_idx  <- length(ids_pages) + 1L
    saveRDS(new_rows, file.path(out_dir, sprintf("ids_page_%03d.rds", next_idx)))
    seen_ids <- c(seen_ids, new_rows$ResultId)
  }

  if (verbose) {
    message(sprintf("  | IDs page %d | raw=%d, new=%d, seen now=%d%s",
                    page, nrow(rows), nrow(new_rows),
                    length(seen_ids),
                    if (!is.na(total)) paste0("/", total) else ""))
  }

  # save checkpoint
  cp <- list(
    seen_ids = seen_ids,
    total    = total,
    query    = q,
    endpoint = endpoint
  )
  saveRDS(cp, checkpoint_path)

  # ----- follow nextLink for THIS run -----
  next_url <- obj[["@odata.nextLink"]] %||% NULL
  while (!is.null(next_url)) {
    obj  <- ln_get_json(
      next_url,
      retry       = retry,
      retry_wait  = retry_wait,
      sleep_after = sleep_after,
      ua          = "lexisbatch/BatchDockets"
    )
    rows <- if (is.null(obj$value)) tibble::tibble() else tibble::as_tibble(obj$value)
    page <- page + 1L

    if (nrow(rows) && "ResultId" %in% names(rows)) {
      new_rows <- rows[!(rows$ResultId %in% seen_ids), , drop = FALSE]
    } else {
      new_rows <- rows
    }

    if (nrow(new_rows)) {
      all_new_ids[[length(all_new_ids)+1L]] <- new_rows
      ids_pages <- list.files(out_dir, pattern = "^ids_page_\\d+\\.rds$")
      next_idx  <- length(ids_pages) + 1L
      saveRDS(new_rows, file.path(out_dir, sprintf("ids_page_%03d.rds", next_idx)))
      seen_ids <- c(seen_ids, new_rows$ResultId)
    }

    if (verbose) {
      message(sprintf("  | IDs page %d | raw=%d, new=%d, seen now=%d%s",
                      page, nrow(rows), nrow(new_rows),
                      length(seen_ids),
                      if (!is.na(total)) paste0("/", total) else ""))
    }

    # update checkpoint
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

  # -----------------------------------------------------------------
  # we now have: possibly some NEW ids this run (in all_new_ids),
  # and all OLD ids on disk as ids_page_XXX.rds
  # strategy:
  #    1. fetch XML for new ids only → save xml_page_XXX.rds
  #    2. load ALL xml_page_XXX.rds → build JSON for everything
  # -----------------------------------------------------------------

  new_id_rows <- dplyr::bind_rows(all_new_ids)
  if (nrow(new_id_rows)) {
    if (verbose) message("== Fetching XML for ", nrow(new_id_rows), " new dockets ...")
    new_xml_rows <- bd_collect_xml_from_ids(
      new_id_rows$ResultId,
      parallel   = parallel_xml,
      workers    = workers,
      page_sleep = page_sleep
    )

    # save new xml page
    xml_pages <- list.files(out_dir, pattern = "^xml_page_\\d+\\.rds$")
    next_xml  <- length(xml_pages) + 1L
    saveRDS(new_xml_rows, file.path(out_dir, sprintf("xml_page_%03d.rds", next_xml)))
  } else {
    if (verbose) message("No new IDs → no new XML to fetch.")
  }

  # now read ALL xml pages and build a single JSON
  xml_files <- list.files(out_dir, pattern = "^xml_page_\\d+\\.rds$", full.names = TRUE)
  if (!length(xml_files)) {
    if (verbose) message("No XML pages saved yet, nothing to parse.")
    return(new_id_rows)
  }

  all_xml_rows <- lapply(xml_files, readRDS)
  all_xml_rows <- dplyr::bind_rows(all_xml_rows)

  if (verbose) message("== Parsing XML → JSON for all saved dockets ...")
  json_blob <- dockets_to_idkeyed_json(all_xml_rows,
                                       id_col  = "ResultId",
                                       xml_col = "xml")

  # write JSON with query name
  json_path <- file.path(out_dir, paste0(safe_search, "_id_keyed.json"))
  writeLines(json_blob, json_path)

  attr(all_xml_rows, "parsed_json") <- json_blob
  all_xml_rows
}
