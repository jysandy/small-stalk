(ns small-stalk.persistence.append-only-log)

(defprotocol AppendOnlyLog
  ;; Readers need to be constructed because there can be multiple readers
  ;; reading the log in different states, but there can only be one writer.
  (new-reader [log] "Returns a new AppendOnlyLogReader instance.")
  (write-entry [log aof-entry] "Writes an entry to the AppendOnlyLog. Thread safety is not guaranteed.")
  (close [log] "Closes the log."))

(defprotocol AppendOnlyLogReader
  (read-entry [reader]))

(defn entry-seq
  [append-only-log-reader]
  (lazy-seq
    (when-let [next-line (read-entry append-only-log-reader)]
      (cons next-line (entry-seq append-only-log-reader)))))

