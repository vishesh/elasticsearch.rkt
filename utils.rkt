#lang racket

(provide (all-defined-out))

(define-syntax (hash-refs stx)
  (syntax-case stx ()
    [(_ h f)
     #'(hash-ref h f)]
    [(_ h f r ...)
     #'(hash-refs (hash-ref h f) r ...)]))

(define (build-path-string f . r)
  (path->string
    (apply build-path
           (filter (λ (x) (not (equal? x "")))
                   (cons f r)))))
