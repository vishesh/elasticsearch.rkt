#lang racket

(require json)
(require net/url
         net/uri-codec
         net/http-client)

(require "utils.rkt")

(provide es/client
         es/client?
         es/DEFAULT-CLIENT
         es/count
         es/do-request
         es/document-delete
         es/document-exist?
         es/document-get
         es/document-index
         es/hits
         es/index-create
         es/index-delete
         es/index-exist?
         es/index-refresh
         es/mapping-get
         es/mapping-put
         es/ping
         es/query-string
         es/search
         es/status
         es/total-hits)


;TODO: Fix data definitions

; Record is jsexpr, interp: actual result documents
; Result is jsexpr, interp: result returned by any ES query

; Client represents an Elasticsearch connection
(struct es/client (host port ssl? username password))

;;; Constants
(define es/DEFAULT-CLIENT (es/client "localhost" 9200 #f #f #f))

; get-rool-url-string : client? -> string
; Returns the elasticsearch server URL
(define (get-root-url-string c)
  (define mode (if (es/client-ssl? c) "https" "http"))
  (define auth
    (if (and (es/client-username c) (es/client-password c))
      (format "~a:~a@" (es/client-username c (es/client-password c))) ""))
  (format "~a://~a~a:~a/" mode auth (es/client-host c) (es/client-port c)))

; port->jsexpr : input-port? -> jsexpr?
; Reads all input from port and convert to jsexpr
(define (port->jsexpr port)
  (string->jsexpr (port->string port)))

; do-request : client? request-type? -> jsexpr?
; Make a request to elasticserch. Path is appended to URL, and type is HTTP
; request type
(define (es/do-request c type path [data ""])
  (define request-fn! (cond
                        [(eqv? type 'GET) get-pure-port]
                        [(eqv? type 'POST)
                         (λ (d)
                           (post-pure-port d (jsexpr->bytes data)))]
                        [(eqv? type 'PUT)
                         (λ (d)
                           (put-pure-port d (jsexpr->bytes data)))]
                        [(eqv? type 'DELETE) delete-pure-port]
                        [else (error "invalid request type")]))
  (define result (port->jsexpr
                   (request-fn!
                     (string->url (string-append (get-root-url-string c)
                                                 path)))))
  (if (hash-ref result 'error #f)
    (error (hash-ref result 'error))
    result))

;; Index functions

; es/index-create : client? string? ... -> bool
; TODO: Add warmers
(define (es/index-create c name
                         #:settings [settings (hash)]
                         #:mappings [mappings (hash)]
                         #:warmers [warmers (hash)]
                         #:aliases [aliases (hash)])
  (hash-ref (es/do-request
              c 'PUT name
              (hash 'settings settings
                    'mappings mappings
                    'warmers warmers
                    'aliases aliases))
            'acknowledged))

; es/index-delete : client? string? -> bool
(define (es/index-delete c name)
  (hash-ref (es/do-request c 'DELETE name)
            'acknowledged))

; es/index-refresh : client? string? -> bool
(define (es/index-refresh c [name #f])
  (define path (if name
                 (format "~a/_refresh" name)
                 "_refresh"))
  (es/do-request c 'POST  path))

; es/index-exist? : client? string? -> bool
; TODO: How to get status code from HTTP request?
(define (es/index-exist? c name)
  (error "Not implemented"))

; es/mappping-put : client? jsexpr? -> bool
; TODO: is giving a type really required? There is mappings case too? Can
;   I send multiple mappings?
(define (es/mapping-put c index type mapping)
  (hash-ref (es/do-request c 'PUT (format "~a/_mapping/~a" index type) mapping)
            'acknowledged))

; es/mapping-get : client? index type mapping
(define (es/mapping-get c index [type ""])
  (es/do-request c 'GET (format "~a/_mapping/~a" index type)))

; es/status : client? [string?] -> jsexpr?
(define (es/status c [index ""])
  ;FIXME: use url joiner
  (es/do-request c 'GET (format "~a/_status" index)))

; es/document-index : client? string? string? jsexpr? serializable? -> string?
; Index or update document. If id is not given, we pass op_type=create
; option to force create
(define (es/document-index c index doctype document #:id [id ""])
  (define method (if (equal? id "") 'POST 'PUT))
  (hash-ref (es/do-request c method
                           (format "~a/~a/~a" index doctype (uri-encode id))
                           document)
            '_id))

; es/document-exist? : client? string? string? string? -> bool
; Return true if document exists
; TODO: Do a HEAD request and get status code
(define (es/document-exist? c index doctype id)
  (error "not implemented"))

; es/document-get? : client? string? string? string? -> maybe<jsexpr?>
; Returns document of given id from given indexn and document collection
; TODO: Add fields parameter
(define (es/document-get c index doctype id)
  (hash-ref
    (es/do-request
      c 'GET (format "~a/~a/~a" index doctype (uri-encode id)))
    '_source #f))

; es/document-delete : client? string? string? string? -> bool
(define (es/document-delete c index doctype id)
  (hash-ref
    (es/do-request c 'DELETE (format "~a/~a/~a" index doctype (uri-encode id)))
    'found))

; query-string : client? string [string?] [string?] -> jsexpr?
; Search query string string q in given ES index and doc type
(define (es/query-string c q #:index [index ""] #:doctype [doctype ""])
  (when (and (false? index) doctype)
    (error "index can't be #f for given document type"))
  (define path (build-path-string index doctype "_search"))
  (es/do-request 
    c 'GET path (hash 'query (hash 'query_string (hash 'query q)))))

; search : client? string? string? jsexpr? -> jsexpr?
(define (es/search c q #:index [index #f] #:doctype [doctype #f])
  (when (and (false? index) doctype)
    (error "index can't be #f for given document type"))
  (define path (build-path-string index doctype "_search"))
  (es/do-request 'GET path q))

; count : client? string? string? jsexpr? -> int
(define (es/count c q #:index [index #f] #:doctype [doctype #f])
  (when (and (false? index) doctype)
    (error "index can't be #f for given document type"))
  (define path (build-path-string index doctype "_count"))
  (hash-ref (es/do-request 'GET path q) 'count))

; es/total-hits : Result -> PosInt
; Return total number of result for our query
(define (es/total-hits res)
  (hash-refs res 'hits 'total))

; es/total-hits : Result -> ListOf<Records>
; Return a list of records
(define (es/hits res)
  (hash-refs res 'hits 'hits))

; ping : client? -> maybe<jsexpr?>
(define (es/ping c)
  (with-handlers
    ([exn:fail:network? (λ (e) #f)])
    (es/do-request c 'GET "/")))
