(ns small-stalk.io
  (:require [clojure.string :as string]
            [small-stalk.failure :as ssf]
            [failjure.core :as f])
  (:import (java.io InputStream OutputStream IOException)))

(defn- decode-char
  "No, this isn't strictly correct, but it'll do for now."
  [a-byte]
  (try
    (char a-byte)
    (catch IllegalArgumentException _
      (ssf/fail {:type ::invalid-character}))))

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
          (f/when-let-ok? [decoded-char (decode-char byte-read)]
            (recur (str buffer decoded-char)
                   read-buffer)))))))

(defn write-crlf-string
  "Appends \r\n to a string and writes it to the output stream."
  [^OutputStream output-stream s]
  (try
    (.write output-stream (.getBytes (str s "\r\n") "US-ASCII"))
    (catch IOException _
      (ssf/fail {:type ::output-stream-closed}))))
