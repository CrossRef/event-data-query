(ns event-data-query.parameters-test
  (:require [clojure.test :refer :all]
            [event-data-query.parameters :as parameters]))

(deftest to-vec
  (testing "to-vec produces a vector representation of the string"
    (is (= (parameters/to-vec "") []) "Empty string returns empty vector")
    (is (= (parameters/to-vec "ONE two 3") [\O \N \E \space \t \w \o \space \3]) "String correctly transformed")))

(deftest read-ident
  (testing "read-ident reads all ident characters from a string and returns tuple"
    ; Not massively useful, but make sure it can cope.
    (is (= (parameters/read-ident (parameters/to-vec ""))
           [[:ident []] []])
        "Reads empty string")

    (is (= (parameters/read-ident (parameters/to-vec "123"))
           [[:ident [\1 \2 \3]] []])
        "Reads terminating string")

    (is (= (parameters/read-ident (parameters/to-vec "123:456"))
           [[:ident [\1 \2 \3]] [\: \4 \5 \6]])
        "Reads string up to delimiter and returns rest")

    (is (= (parameters/read-ident (parameters/to-vec "123,456"))
           [[:ident [\1 \2 \3]] [\, \4 \5 \6]])
        "Reads string up to separator and returns rest")))


(deftest read-quoted-ident
  (testing "read-quoted-ident reads all ident characters within the quotes stipulated by first character from a string and returns tuple"
    ; Again, make sure it can cope.
    (is (= (parameters/read-quoted-ident (parameters/to-vec ""))
           [[:ident []] []])
        "Reads empty string")

    (is (= (parameters/read-quoted-ident (parameters/to-vec "\"123\""))
           [[:ident [\1 \2 \3]] []])
        "Reads terminating string using double quotes")

    (is (= (parameters/read-quoted-ident (parameters/to-vec "'123'"))
           [[:ident [\1 \2 \3]] []])
        "Reads terminating string using single quotes")

    (is (= (parameters/read-quoted-ident (parameters/to-vec "X123X"))
           [[:ident [\1 \2 \3]] []])
        "Reads terminating string using any quote character")

    ; Input is: "123\"456"
    (is (= (parameters/read-quoted-ident (parameters/to-vec "\"123\\\"456\""))
           [[:ident [\1 \2 \3 \" \4 \5 \6]] []])
        "Reads quoted quotes in identifier when the ident terminates the string.")

    ; Input is: '123\'456'
    (is (= (parameters/read-quoted-ident (parameters/to-vec "'123\\'456'"))
           [[:ident [\1 \2 \3 \' \4 \5 \6]] []])
        "Reads quoted other kinds of quotes in identifier when the ident terminates the string.")

    ; Input is: '123\\456'
    (is (= (parameters/read-quoted-ident (parameters/to-vec "'123\\\\456'"))
           [[:ident [\1 \2 \3 \\ \4 \5 \6]] []])
        "Allows escaped backslash.")

    ; Input is: '123\"456'
    (is (thrown? IllegalArgumentException
          (parameters/read-quoted-ident (parameters/to-vec "\"123\\'456\"")))
      "Only the specified quote is allowed to be escaped.")

    ; Input is: "123\"456"
    (is (= (parameters/read-quoted-ident (parameters/to-vec "\"123\\\"456\"and more"))
           [[:ident [\1 \2 \3 \" \4 \5 \6]] [\a \n \d \space \m \o \r \e]])
        "Reads quoted quotes in identifier when the ident doesn't terminate the string.")

    ; Input is: '123\'456'
    (is (= (parameters/read-quoted-ident (parameters/to-vec "'123\\'456'and more"))
           [[:ident [\1 \2 \3 \' \4 \5 \6]] [\a \n \d \space \m \o \r \e]])
        "Reads quoted other kinds of quotes in identifier when the ident doesn't terminate the string.")))
 
 (deftest scan
  (testing "scan can read all types of token types from a string, returning strings"
    (is (= (parameters/scan "12:34,\"56\":'7\\\\8'")
        [[:ident [\1 \2]]
         [:delimiter \:]
         [:ident [\3 \4]]
         [:separator \,]
         [:ident [\5 \6]]
         [:delimiter \:]
         [:ident [\7 \\ \8]]]))))

(deftest parse
  (testing "parse can parse input strings to param dicts, throwing appropriate format errors"
    (is (= (parameters/parse "12:34") {"12" "34"}))
    (is (= (parameters/parse "12:34,56:78") {"12" "34" "56" "78"}))

    (is (= (parameters/parse "12:34,'56':78") {"12" "34" "56" "78"}))
    (is (= (parameters/parse "12:34,'5\\'6':78") {"12" "34" "5'6" "78"}))

    (is (= (parameters/parse "12:34,\"56\":78") {"12" "34" "56" "78"}))
    (is (= (parameters/parse "12:34,\"5\\\"6\":78") {"12" "34" "5\"6" "78"}))
    (is (= (parameters/parse "12:34,\"5\\\\6\":78") {"12" "34" "5\\6" "78"}))

    ; This is the reason we're here.
    (is (= (parameters/parse "subj-id:'http://www.example.com',obj-id:whatevs") {"subj-id" "http://www.example.com" "obj-id" "whatevs"}))

    (is (thrown? IllegalArgumentException (parameters/parse "12:34,56:78,12:34")) "Duplicate keys should be handled.")))


