# R/auth.R

#' Authenticate to LexisNexis with a client id/secret
#'
#' Stores the OAuth token in a global and in LN_TOKEN env var
#' so other functions can reuse it.
#' @param key Client ID
#' @param secret Client secret
#' @export
lexis_auth <- function(key, secret) {
  lexis <- httr::oauth_endpoint(
    request   = "https://auth-api.lexisnexis.com/oauth/v2/token",
    authorize = "https://auth-api.lexisnexis.com/oauth/v2/authorize",
    access    = "https://auth-api.lexisnexis.com/oauth/v2/token"
  )
  auth.code <<- httr::oauth2.0_token(
    endpoint           = lexis,
    app                = httr::oauth_app(appname = "lexis_api", key = key, secret = secret),
    scope              = "http://oauth.lexisnexis.com/all",
    client_credentials = TRUE,
    use_basic_auth     = TRUE,
    user_params        = lexis,
    cache              = FALSE
  )
  Sys.setenv(LN_TOKEN = auth.code$credentials$access_token)
}

#' Authenticate using env vars LEXIS_CLIENT_ID / LEXIS_CLIENT_SECRET
#' @export
lexis_auth_env <- function() {
  key <- Sys.getenv("LEXIS_CLIENT_ID")
  sec <- Sys.getenv("LEXIS_CLIENT_SECRET")
  if (!nzchar(key) || !nzchar(sec)) {
    stop("Set LEXIS_CLIENT_ID and LEXIS_CLIENT_SECRET env vars.")
  }
  lexis_auth(key, sec)
}

# shared base
LN_BASE_ROOT <- "https://services-api.lexisnexis.com"

# get bearer from env or global token
ln_get_bearer <- function() {
  tok <- Sys.getenv("LN_TOKEN", "")
  if (nzchar(tok)) return(tok)
  if (exists("auth.code", inherits = TRUE)) {
    cred <- tryCatch(get("auth.code", inherits = TRUE)$credentials$access_token,
                     error = function(e) "")
    if (nzchar(cred)) return(cred)
  }
  stop("No token found. Call lexis_auth_env() or lexis_auth(key, secret).")
}

# always build /v1/... URLs
ln_build_url <- function(endpoint, query = NULL) {
  ep <- paste0("v1/", sub("^/+", "", endpoint))
  httr::modify_url(LN_BASE_ROOT, path = ep, query = query)
}

# small helpers used by all 3 modules
`%||%` <- function(a,b) if (!is.null(a)) a else b

make_safe_name <- function(x) {
  if (is.null(x) || !nzchar(x)) return("no_search")
  x <- gsub("[^A-Za-z0-9._-]+", "_", x)
  substr(x, 1, 80)
}

safe_read_rds <- function(path) {
  if (file.exists(path)) {
    tryCatch(readRDS(path), error = function(e) NULL)
  } else {
    NULL
  }
}

#' GET JSON with bearer and basic retry
#' @keywords internal
#' @export
ln_get_json <- function(endpoint_or_full,
                        query       = NULL,
                        retry       = 3,
                        retry_wait  = 1,
                        sleep_after = 0,
                        ua          = "lexisbatch/R") {
  bearer <- ln_get_bearer()
  url <- if (grepl("^https?://", endpoint_or_full)) {
    endpoint_or_full
  } else {
    ln_build_url(endpoint_or_full, query)
  }

  tries <- 0L
  repeat {
    resp <- httr::GET(
      url,
      httr::add_headers(
        Authorization = paste("Bearer", bearer),
        Accept        = "application/json",
        `User-Agent`  = ua
      )
    )
    if (!httr::http_error(resp)) break
    code <- httr::status_code(resp)
    if (!(code %in% c(429, 500, 502, 503, 504)) || tries >= retry) {
      txt <- tryCatch(httr::content(resp, as="text", encoding="UTF-8"), error = function(e) "")
      stop(sprintf("HTTP %s\n%s", code, txt))
    }
    tries <- tries + 1L
    Sys.sleep(retry_wait * tries)
  }

  out <- jsonlite::fromJSON(httr::content(resp, as="text", encoding="UTF-8"), flatten = TRUE)
  if (sleep_after > 0) Sys.sleep(sleep_after)
  out
}

# we also need XML getter for cases & dockets
#' @keywords internal
#' @export
ln_get_xml <- function(media_url, ua = "lexisbatch/R") {
  bearer <- ln_get_bearer()
  full <- if (grepl("^https?://", media_url)) media_url else ln_build_url(media_url)
  resp <- httr::GET(
    full,
    httr::add_headers(
      Authorization = paste("Bearer", bearer),
      Accept        = "application/atom+xml",
      `User-Agent`  = ua
    )
  )
  if (httr::http_error(resp)) return(NULL)
  xml2::read_xml(httr::content(resp, as = "raw"))
}
