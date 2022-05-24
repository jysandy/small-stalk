(ns small-stalk.commands.parsing
  (:require [failjure.core :as f]
            [small-stalk.failure :as ssf]))

(defn parser-failure [map]
  (-> map
      (assoc :type ::parser-failure)
      (ssf/fail)))

(defn parser-failure? [e]
  (and (f/failed? e)
       (= ::parser-failure (:type e))))

(defmulti convert-and-parse (fn [type _token] type))

(defmethod convert-and-parse ::non-negative-number
  [_type token]
  (try
    (let [num (Long/parseLong token)]
      (if (neg? num)
        (parser-failure {:name ::unexpected-negative-number})
        num))
    (catch NumberFormatException _
      (parser-failure {:name ::parsing-number-failed}))))

(defn parse-args [args-grammar tokens]
  (cond
    (< (count tokens) (count args-grammar)) (parser-failure {:name ::too-few-args})
    (> (count tokens) (count args-grammar)) (parser-failure {:name ::too-many-args})
    :else (f/ok->> tokens
                   (map vector args-grammar)
                   (map (fn [[[key-name type] token]]
                          (f/attempt-all [parsed-arg (convert-and-parse type token)]
                            [key-name parsed-arg]
                            (f/when-failed [e]
                              (if (parser-failure? e)
                                (assoc e :key key-name)
                                e)))))
                   ssf/first-failure
                   (into {}))))

(def all-grammars {"put"                  [[:priority ::non-negative-number] [:time-to-run-secs ::non-negative-number]]
                   "peek-ready"           []
                   "reserve"              []
                   "delete"               [[:job-id ::non-negative-number]]
                   "release"              [[:job-id ::non-negative-number] [:new-priority ::non-negative-number]]
                   "reserve-with-timeout" [[:timeout-secs ::non-negative-number]]})

(defn parse-command [[command-name & arg-tokens :as _tokens]]
  (f/attempt-all [args-grammar (or (get all-grammars
                                        command-name)
                                   (parser-failure {:name ::unknown-command}))
                  parsed-args  (parse-args args-grammar
                                           arg-tokens)]

    (into {:command-name command-name}
          parsed-args)))
