elasticsearch.rkt
=================

A simple straighforward and low-level API for Elasticsearch. The
implementation is incomplete and things are likely to change.

Installation
-----------

Examples
--------

```racket
(require (prefix-in es/ elasticsearch))

; the client structure required by all ES function
(struct es/client (host port ssl? username password))

# this is provided as constant es/DEFAULT-CLIENT
(define DC (es/client "localhost" "9200" #f #f #f))

(es/index-create "twitter") ; returns jsexpr returned by ES

; delete index, return true if successful
(es/index-delete C "twitter")

; create a mapping. returns true if successful
(es/mapping-put
  DC "twitter" "tweet"
  (hash
    'tweet (hash
             'properties (hash
                           'user (hash
                                   'type "string"
                                   'store #t
                                   'index "not_analyzed")
                           'text (hash
                                   'type "string"
                                   'store #t
                                   'index "analyzed")))))

; index (create or partially update) a document.
; Returns ID of the document
(es/document-index
  DC "twitter" "tweet"
  (hash 'user "foo"
        'text "Today is a very good day"))

; can also specify id of document specifically
(es/document-index
  DC "twitter" "tweet"
  (hash 'user "foo"
        'text "Today is a very good day")
  #:id 2)

; get document with id=2
(es/document-get DC "twitter" "tweet" 2)

; delete document with id=2. returns true if successful
(es/document-delete DC "twitter" "tweet" 2)

; Search with a query string. returns hits as jsexpr
(es/query-string DC "programming racket" #:index "twitter" #:doc-type "tweet")

; you could omitt document type and search in all documents of 
; given index
(es/query-string DC "programming racket" #:index "twitter")

; or you could search in all indexes present in ES
(es/query-string DC "programming racket")

; For using ES Query DSL
(es/search DC
  (hash
    'query (hash
             'match (hash
                      'text "racket")))
  #:index "twitter" #:doctype "tweet")
```

License
------
3-clause BSD. See COPYING file in source tree.
