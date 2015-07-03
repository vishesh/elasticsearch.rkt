#lang racket

(require rackunit
         json
         (prefix-in es/ "elasticsearch.rkt"))

(test-begin
  (define C es/DEFAULT-CLIENT)
  (define INDEX-NAME "rkt-test-index")
  (define DOC-TYPE "rkt-doc-type")
  (define TEXT1 "one two three four hello world")
  (define TEXT2 "computer language programming hello")

  (check-pred jsexpr? (es/ping C))
  #|(check-pred jsexpr? (es/index-create C "rkt-test-index"))|#
  #|(check-exn exn:fail? (es/index-create C "visheshyadav"))|#

  (check-equal? (es/document-index C INDEX-NAME DOC-TYPE
                                   (hash 'title "numbers"
                                         'text TEXT1)
                                   #:id "1")
                "1")

  (check-equal? (es/document-index C INDEX-NAME DOC-TYPE
                                   (hash 'title "numbers"
                                         'text TEXT2)
                                   #:id "2")
                "2")

  (define document (es/document-get C INDEX-NAME DOC-TYPE "1"))
  (check-equal? (hash-ref document 'title #f) "numbers")
  (check-equal? (hash-ref document 'text #f) TEXT1)

  (check-pred jsexpr? (es/index-refresh C INDEX-NAME))
  (check-true (es/document-delete C INDEX-NAME DOC-TYPE "1"))
  (check-true (es/document-delete C INDEX-NAME DOC-TYPE "2"))

  (check-true (es/index-delete C INDEX-NAME)))
