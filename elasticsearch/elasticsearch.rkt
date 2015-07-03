#lang racket

(require json)
(require net/url
         net/uri-codec
         net/http-client)

(require "utils.rkt")

(provide client
         client?
         DEFAULT-CLIENT
         count
         do-request
         document-delete
         document-exist?
         document-get
         document-index
         hits
         index-create
         index-delete
         index-exist?
         index-refresh
         mapping-get
         mapping-put
         ping
         query-string
         search
         status
         total-hits)


;TODO: Fix data definitions

; Record is jsexpr, interp: actual result documents
; Result is jsexpr, interp: result returned by any ES query

; Client represents an Elasticsearch connection
(struct client (host port ssl? username password))

;;; Constants
(define DEFAULT-CLIENT (client "localhost" 9200 #f #f #f))

; get-rool-url-string : client? -> string
; Returns the elasticsearch server URL
(define (get-root-url-string c)
  (define mode (if (client-ssl? c) "https" "http"))
  (define auth
    (if (and (client-username c) (client-password c))
      (format "~a:~a@" (client-username c (client-password c))) ""))
  (format "~a://~a~a:~a/" mode auth (client-host c) (client-port c)))

; port->jsexpr : input-port? -> jsexpr?
; Reads all input from port and convert to jsexpr
(define (port->jsexpr port)
  (string->jsexpr (port->string port)))

; do-request : client? request-type? -> jsexpr?
; Make a request to elasticserch. Path is appended to URL, and type is HTTP
; request type
(define (do-request c type path [data ""])
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

; index-create : client? string? ... -> bool
; TODO: Add warmers
(define (index-create c name
                         #:settings [settings (hash)]
                         #:mappings [mappings (hash)]
                         #:warmers [warmers (hash)]
                         #:aliases [aliases (hash)])
  (hash-ref (do-request
              c 'PUT name
              (hash 'settings settings
                    'mappings mappings
                    'warmers warmers
                    'aliases aliases))
            'acknowledged))

; index-delete : client? string? -> bool
(define (index-delete c name)
  (hash-ref (do-request c 'DELETE name)
            'acknowledged))

; index-refresh : client? string? -> bool
(define (index-refresh c [name #f])
  (define path (if name
                 (format "~a/_refresh" name)
                 "_refresh"))
  (do-request c 'POST  path))

; index-exist? : client? string? -> bool
; TODO: How to get status code from HTTP request?
(define (index-exist? c name)
  (error "Not implemented"))

; mappping-put : client? jsexpr? -> bool
; TODO: is giving a type really required? There is mappings case too? Can
;   I send multiple mappings?
(define (mapping-put c index type mapping)
  (hash-ref (do-request c 'PUT (format "~a/_mapping/~a" index type) mapping)
            'acknowledged))

; mapping-get : client? index type mapping
(define (mapping-get c index [type ""])
  (do-request c 'GET (format "~a/_mapping/~a" index type)))

; status : client? [string?] -> jsexpr?
(define (status c [index ""])
  ;FIXME: use url joiner
  (do-request c 'GET (format "~a/_status" index)))

;; Document API

; document-index : client? string? string? jsexpr? serializable? -> string?
; Index or update document. If id is not given, we pass op_type=create
; option to force create
(define (document-index c index doctype document #:id [id ""])
  (define method (if (equal? id "") 'POST 'PUT))
  (hash-ref (do-request c method
                           (format "~a/~a/~a" index doctype (uri-encode id))
                           document)
            '_id))

; document-exist? : client? string? string? string? -> bool
; Return true if document exists
; TODO: Do a HEAD request and get status code
(define (document-exist? c index doctype id)
  (error "not implemented"))

; document-get? : client? string? string? string? -> maybe<jsexpr?>
; Returns document of given id from given indexn and document collection
; TODO: Add fields parameter
(define (document-get c index doctype id)
  (hash-ref
    (do-request
      c 'GET (format "~a/~a/~a" index doctype (uri-encode id)))
    '_source #f))

; document-delete : client? string? string? string? -> bool
(define (document-delete c index doctype id)
  (hash-ref
    (do-request c 'DELETE (format "~a/~a/~a" index doctype (uri-encode id)))
    'found))

;; Searching
;
; query-string : client? string [string?] [string?] -> jsexpr?
; Search query string string q in given ES index and doc type
(define (query-string c q #:index [index ""] #:doctype [doctype ""])
  (when (and (false? index) doctype)
    (error "index can't be #f for given document type"))
  (define path (build-path-string index doctype "_search"))
  (do-request 
    c 'GET path (hash 'query (hash 'query_string (hash 'query q)))))

; search : client? string? string? jsexpr? -> jsexpr?
(define (search c body #:index [index #f] #:doctype [doctype #f])
  (when (and (false? index) doctype)
    (error "index can't be #f for given document type"))
  (define path (build-path-string index doctype "_search"))
  (do-request 'GET path body))

; count : client? string? string? jsexpr? -> int
(define (count c body #:index [index #f] #:doctype [doctype #f])
  (when (and (false? index) doctype)
    (error "index can't be #f for given document type"))
  (define path (build-path-string index doctype "_count"))
  (hash-ref (do-request 'GET path body) 'count))

; total-hits : Result -> PosInt
; Return total number of result for our query
(define (total-hits res)
  (hash-refs res 'hits 'total))

; total-hits : Result -> ListOf<Records>
; Return a list of records
(define (hits res)
  (hash-refs res 'hits 'hits))

; ping : client? -> maybe<jsexpr?>
(define (ping c)
  (with-handlers
    ([exn:fail:network? (λ (e) #f)])
    (do-request c 'GET "/")))
