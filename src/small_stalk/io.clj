(ns small-stalk.io
  (:require [clojure.string :as string]
            [small-stalk.failure :as ssf])
  (:import (java.io InputStream OutputStream IOException)))

(defn read-string-until-crlf
  "Reads a string from the input stream until CRLF or EOF is encountered.
  Includes the CRLF."
  [^InputStream input-stream]
  (loop [buffer      ""
         read-buffer (byte-array [0])]
    (if (string/ends-with? buffer "\r\n")
      buffer
      (let [num-bytes-read (.read input-stream read-buffer)
            byte-read      (first read-buffer)]
        (if (= num-bytes-read -1)
          ;; EOF was reached.
          (ssf/fail {:type           ::eof-reached
                     :remaining-data buffer})
          (recur (str buffer (char byte-read))
                 read-buffer))))))

(defn write-crlf-string
  "Appends \r\n to a string and writes it to the output stream."
  [^OutputStream output-stream s]
  (try
    (.write output-stream (.getBytes (str s "\r\n") "US-ASCII"))
    (catch IOException _
      (ssf/fail {:type ::output-stream-closed}))))
