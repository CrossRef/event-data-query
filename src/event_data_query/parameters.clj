(ns event-data-query.parameters
  "Parse filter parameters from query strings in key:value,key:value format into a map. Allow quoted keys or values."
  (:require [clojure.set :refer [union]]))

(def delimiters #{\:})
(def quotes #{\' \"})
(def separators #{\,})

(def ident-delimiter (union delimiters quotes separators #{\\}))

(defn to-vec
  [string]
  (vec (.toCharArray string)))

(defn read-ident
  [input]
  [[:ident (take-while (comp not ident-delimiter) input)]
   (drop-while (comp not ident-delimiter) input)])

(defn read-quoted-ident
  [[delimiter & input]]
  (let [escapable #{\\ delimiter}]
    ; (prn "escapable" escapable)
    (loop [acc []
           escape false
           c (first input)
           cs (rest input)]

      (when (and escape (not (escapable c)))
        (throw (new IllegalArgumentException (str "Unexpected escaped character: " c " expected one of: " (clojure.string/join " or " (vec escapable))))))

      ; End of input?
      (if (nil? c)
        [[:ident acc] cs]
        (if (= \\ c)
          
          (if escape
            ; Escaped slash
            (recur (conj acc c) false (first cs) (rest cs))
            ; Start of escape.
            (recur acc true (first cs) (rest cs)))

          (if escape
            ; If we're mid-escape-sequence, continue.
            ; (Earlier check that this was a valid escape sequence).
            (recur (conj acc c) false (first cs) (rest cs))

            (if (= delimiter c)
              ; If we hit a delimiter outside an escape sequence, it's time to stop.
              [[:ident acc] cs]

              ; Otherwise, continue.
              (recur (conj acc c) false (first cs) (rest cs)))))))))


(defn read-token
  "Read a token.
   Return tuple of [[type value] rest]. "
  [cs]
  (let [c (first cs)]
    (cond
      (empty? cs) nil
      (quotes c) (read-quoted-ident cs)
      (delimiters c) [[:delimiter c] (rest cs)]
      (separators c) [[:separator c] (rest cs)]
      true (read-ident cs))))

(defn scan
  [input-str]
  (loop [input (to-vec input-str)
         acc []]
    (if-let [[token rest-tokens] (read-token input)]
      (recur rest-tokens (conj acc token))
      acc)))

(defn parse
  [input-str]
  (let [tokens (scan input-str)]
    ; Any of these will be nil at destructuring time.
    ; So at the end of iteration, k-typ will be nil.
    ; s-typ will also be nil if there's no terminating separator.
    (loop [[[k-typ k-val] [d-typ d-val] [v-typ v-val] [s-typ s-val] & rest-tokens] tokens
           acc {}]

      (if (nil? k-typ)
        acc
        (do 

      (when-not (= :ident k-typ)
        (throw (new IllegalArgumentException (str "Expected key but got" v-val))))

      (when-not (= :delimiter d-typ)
        (throw (new IllegalArgumentException (str "Expected delimiter ':' but got:" d-val))))

      (when-not (= :ident v-typ)
        (throw (new IllegalArgumentException (str "Expected value but got:" d-val))))

      (when (and s-typ (not= :separator s-typ))
        (throw (new IllegalArgumentException (str "Expected separator ',' but got:" s-val))))

      (let [k-str-val (apply str k-val)
            v-str-val (apply str v-val)]

      (when (acc k-str-val)
        (throw (new IllegalArgumentException (str "Got duplicate key:" k-str-val))))

      (recur rest-tokens (assoc acc k-str-val v-str-val))))))))

    