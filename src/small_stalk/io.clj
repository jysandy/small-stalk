(ns small-stalk.io
  (:require [clojure.string :as string])
  (:import (java.io InputStream)))

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
          ;; EOF was reached. Return the rest of the buffer now.
          buffer
          (recur (str buffer (char byte-read))
                 read-buffer))))))